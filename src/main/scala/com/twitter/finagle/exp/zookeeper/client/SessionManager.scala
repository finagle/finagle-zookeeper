package com.twitter.finagle.exp.zookeeper.client

import com.twitter.finagle.exp.zookeeper._
import com.twitter.conversions.time._
import com.twitter.util._
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.Throw
import com.twitter.finagle.exp.zookeeper.ConnectRequest
import com.twitter.finagle.exp.zookeeper.watch.WatchManager
import java.util.concurrent.atomic.AtomicBoolean

class SessionManager(
  requestWriter: Request => Future[Response],
  watchManagr: WatchManager,
  autoReconnection: Boolean = true) {

  /**
   * Session variables
   * sessionId - current session Id
   * sessionPassword - current session password
   * sessionTimeout - negotiated session timeout
   * xid - current request Id
   * lastZxid - last ZooKeeper Transaction Id
   */
  private[this] var sessionId: Long = 0
  private[this] var sessionPassword: Array[Byte] = Array[Byte](16)
  private[this] var sessionTimeOut: Int = 0
  private[this] var xid: Int = 2
  @volatile var lastZxid: Long = 0L

  /**
   * State variables
   * state - describes the current state of the connection
   * isFirstConnect - if it's the first connection
   * autoReconnect - if you want to reconnect in case of connection loss
   */
  @volatile private[this] var state: States.ConnectionState = States.NOT_CONNECTED
  private[this] var isFirstConnect: Boolean = true
  private[this] val isClosingSession = new AtomicBoolean(false)
  private[this] var autoReconnect: Boolean = autoReconnection

  /**
   * Local variables
   * writer - use to write requests to the transport
   * watchManager - use to get watches in case of reconnection
   * pingScheduler - use to send ping request every x milliseconds
   */
  private[this] val writer: Request => Future[Response] = requestWriter
  private[this] val watchManager = watchManagr
  private[this] val pingScheduler = new PingScheduler

  /**
   * States describes every possible states of the connection
   */
  object States extends Enumeration {
    type ConnectionState = Value
    val CONNECTING, ASSOCIATING, CONNECTED, CONNECTED_READONLY, CLOSED, AUTH_FAILED, NOT_CONNECTED = Value
    val CONNECTION_LOST, SESSION_EXPIRED, SESSION_MOVED, SASL_AUTHENTICATED = Value
  }

  /**
   * This is how we start to send ping every x milliseconds
   * @param f a request writer taking a PingRequest as argument
   */
  private[this] def startPing(f: => Unit) = pingScheduler(realTimeout.milliseconds)(f)
  /**
   * Classic way to determine the real interval between two pings
   * @return effective period to send ping
   */
  private[this] def realTimeout: Long = sessionTimeOut * 2 / 3

  /**
   * Classic way to check a response header ( error field )
   * This will first parse the header, the state will be changed if required
   * then checkState will verify if the connection is still alive
   * @param header a response header
   */
  def apply(header: ReplyHeader) {
    checkHeader(header)
    checkState()
  }

  /**
   * Classic way to check a watch event ( state field )
   * This will first parse the WatchEvent, the state will be changed if required
   * then checkState will verify if the connection is still alive
   * @param event a watch event
   */
  def apply(event: WatchEvent) {
    checkWatchEvent(event)
    checkState()
  }

  /**
   * Connection management :
   * -- prepareConnection
   * It is called when the connectRequest is written in the Transport
   *
   * -- completeConnection
   * Called if the connection succeeded (after connectResponse decoding)
   *
   * -- connectionFailed
   * Called if the connection is not established
   */
  def prepareConnection() { state = States.CONNECTING }

  def completeConnection(response: ConnectResponse) {
    if (isFirstConnect) isFirstConnect = false
    sessionId = response.sessionId
    sessionTimeOut = response.timeOut
    sessionPassword = response.passwd
    state = States.CONNECTED
    startPing(writer(new PingRequest))
  }

  def connectionFailed() {
    state = States.CLOSED
  }

  /**
   * Close session management :
   * -- prepareCloseSession
   * Called before the close session request is written
   *
   * -- completeCloseSession
   * Called after the close session response decoding
   */
  def prepareCloseSession() {
    pingScheduler.currentTask.get.cancel()
    isClosingSession.set(true)
  }

  def completeCloseSession() {
    state = States.CLOSED
  }

  def isClosing: Boolean = {
    isClosingSession.get()
  }

  /**
   * This method is called each time we try to write on the Transport
   * to make sure the connection is still alive. If it's not then it can
   * try to reconnect ( if the session has not expired ) or create a new session
   * if the session has expired. It won't connect if the client has never connected
   */
  def checkState() {
    if (!isFirstConnect) {
      if (state == States.CONNECTION_LOST) {
        // We can try to reconnect with last zxid and set the watches back
        pingScheduler.currentTask.get.cancel()
        reconnect

      } else if (state == States.SESSION_MOVED) {
        // The session has moved to another server
        // TODO what ?

      } else if (state == States.SESSION_EXPIRED) {
        // Reconnect with a new session
        pingScheduler.currentTask.get.cancel()
        connect

      } else if (state != States.CONNECTED) {
        // TRY to reconnect with a new session
        throw new RuntimeException("Client is not connected, see SessionManager")
      }
    } else {
      // TODO this is false while pinging RW server
      if (state != States.CONNECTING)
        throw new RuntimeException("No connection exception: Did you ever connected to the server ? " + state)
    }
  }

  /**
   * Before connection, we check if we are already trying to connect, or
   * if the connection is already established
   */
  def checkStateBeforeConnect() {
    if (!isFirstConnect && state == States.CONNECTED || isFirstConnect && state == States.CONNECTED)
      throw new RuntimeException("You are already connected ! don't try to connect")
    else if (isFirstConnect && state == States.CONNECTING)
      throw new RuntimeException("Connection in progress ! don't try to connect")
  }

  /**
   * Here we are parsing the header's error field
   * and changing the connection state if required
   * then the ZXID is updated.
   * @param header a request header
   */
  def checkHeader(header: ReplyHeader) {
    header.err match {
      case 0 => state = States.CONNECTED // TODO not if readonly manage this
      case -4 => state = States.CONNECTION_LOST
      case -112 => state = States.SESSION_EXPIRED
      case -118 => state = States.SESSION_MOVED
      case -666 =>
        // -666 is the code for first connect
        if (state == States.CONNECTING) {
          isFirstConnect = false
          state = States.CONNECTED
        }
      case _ =>
    }
    lastZxid = header.zxid
    println("-->Header reply | XID: " + header.xid + " | ZXID: " + header.zxid + " | ERR: " + header.err)
  }

  /**
   * Here we are parsing the watchEvent's state field
   * and changing the connection state if required
   * @param event a request header
   */
  def checkWatchEvent(event: WatchEvent) {
    event.state match {
      case 0 => state = States.NOT_CONNECTED
      case 3 => state = States.CONNECTED
      case -112 => state = States.SESSION_EXPIRED
    }
  }

  /**
   * We try to reconnect with the last session informations,
   * if the connection can't be restablished (maybe session has expired)
   * then a new connection is tried.
   * @return Future[Unit]
   */
  def reconnect: Future[Unit] = {
    val recoReq = new ConnectRequest(0, lastZxid, sessionTimeOut, sessionId, sessionPassword)
    prepareConnection()
    Try { writer(recoReq) } match {
      case Return(rep) =>
        rep flatMap { connectResponse =>
          completeConnection(connectResponse.asInstanceOf[ConnectResponse])
          // TODO do we need to set watches back if the session has not expired
          Future(Unit)
        }
      case Throw(exc) =>
        // New connection
        connect.unit
    }
  }

  /**
   * We try to create a new session
   * @return Future[Unit]
   */
  def connect: Future[Unit] = {
    val recoReq = new ConnectRequest(connectionTimeout = sessionTimeOut)
    prepareConnection()
    Try { writer(recoReq) } match {
      case Return(rep) =>
        rep flatMap { connectResponse =>
          // TODO set watches back if it's a reconnection
          completeConnection(connectResponse.asInstanceOf[ConnectResponse])
          Future(Unit)
        }
      case Throw(exc) =>
        state = States.CLOSED
        throw exc
    }
  }

  /**
   * To get the next xid
   * @return the next unique XID
   */
  def getXid: Int = {
    this.synchronized {
      xid += 1
      xid - 1
    }
  }
}

/**
 * PingScheduler is used to send Ping Request to the server
 * every x milliseconds, if the connection is alive. If we issue
 * a connection loss then the current task is cancelled and a new one
 * is created after the reconnection succeeded.
 */
class PingScheduler {
  /**
   * Local variables
   * timer - default Twitter timer ( never stop it! )
   * currentTask - the last scheduled timer's task
   */
  val timer = DefaultTimer
  var currentTask: Option[TimerTask] = None

  def apply(period: Duration)(f: => Unit) {
    currentTask = Some(timer.twitter.schedule(period)(f))
  }

  def updateTimer(period: Duration)(f: => Unit) = {
    currentTask.get.cancel()
    apply(period)(f)
  }
}