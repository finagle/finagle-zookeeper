package com.twitter.finagle.exp.zookeeper.client

import com.twitter.finagle.exp.zookeeper._
import com.twitter.conversions.time._
import com.twitter.util._
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.Throw
import com.twitter.finagle.exp.zookeeper.ConnectRequest
import com.twitter.finagle.exp.zookeeper.watch.WatchManager

// TODO check synchronization of variables and functions
// TODO stop the ping timer when the connection is down
class SessionManager(
  requestWriter: Request => Future[Response],
  watchManagr: WatchManager,
  autoReconnection: Boolean = true) {

  var sessionId: Long = 0
  var timeOut: Int = 0
  var passwd: Array[Byte] = Array[Byte](16)
  val pingTimer = new PingTimer
  @volatile var writer: Request => Future[Response] = requestWriter
  @volatile var lastZxid: Long = 0L
  @volatile private[this] var state: states.ConnectionState = states.NOT_CONNECTED
  @volatile private[this] var xid: Int = 2
  private[this] var isFirstConnect: Boolean = true
  private[this] var autoReconnect: Boolean = autoReconnection
  private[this] val watchManager = watchManagr

  object states extends Enumeration {
    type ConnectionState = Value
    val CONNECTING, ASSOCIATING, CONNECTED, CONNECTED_READONLY, CLOSED, AUTH_FAILED, NOT_CONNECTED = Value
    val CONNECTION_LOST, SESSION_EXPIRED, SESSION_MOVED, SASL_AUTHENTICATED = Value
  }

  private[this] def startPing(f: => Unit) = pingTimer(realTimeout.milliseconds)(f)
  private[this] def realTimeout: Long = timeOut * 2 / 3

  def apply(header: ReplyHeader) {
    checkHeader(header)
    checkState()
  }

  def apply(event: WatchEvent) {
    checkWatchEvent(event)
    checkState()
  }

  // TODO Créer une fonction spéciale pour la premiere connection, changer l'état de isFirstConnect après que la connection soit établie

  def getXid: Int = {
    this.synchronized {
      xid += 1
      xid - 1
    }
  }

  def prepareConnection() { state = states.CONNECTING }

  def connectionFailed() {
    state = states.CLOSED
  }

  def completeConnection(response: ConnectResponse) {
    if (isFirstConnect) isFirstConnect = false
    sessionId = response.sessionId
    timeOut = response.timeOut
    passwd = response.passwd
    state = states.CONNECTED
    startPing(writer(new PingRequest))
  }

  def prepareCloseSession() {
    pingTimer.currentTask.get.cancel()
  }

  def completeCloseSession() {
    state = states.CLOSED
  }

  def checkState() {

    if (!isFirstConnect) {
      if (state == states.CONNECTION_LOST) {
        // We can try to reconnect with last zxid and set the watches back
        reconnect

      } else if (state == states.SESSION_MOVED) {
        // The session has moved to another server
        // TODO

      } else if (state == states.SESSION_EXPIRED) {
        // Reconnect with a new session
        connect

      } else if (state != states.CONNECTED) {
        // TRY to reconnect with a new session
        throw new RuntimeException("Client is not connected, see SessionManager")
      }
    } else {
      if (state != states.CONNECTING)
        throw new RuntimeException("No connection exception: Did you connect to the server ? " + state)
    }

  }

  def checkStateBeforeConnect() {
    if (!isFirstConnect && state == states.CONNECTED || isFirstConnect && state == states.CONNECTED)
      throw new RuntimeException("You are already connected ! don't try to connect")
    else if (isFirstConnect && state == states.CONNECTING)
      throw new RuntimeException("Connection in progress ! don't try to connect")
  }

  def checkHeader(header: ReplyHeader) {
    header.err match {
      case 0 => state = states.CONNECTED // TODO not if readonly manage this
      case -4 => state = states.CONNECTION_LOST
      case -112 => state = states.SESSION_EXPIRED
      case -118 => state = states.SESSION_MOVED
      case -666 =>
        // ERR code for first connect
        if (state == states.CONNECTING) {
          isFirstConnect = false
          state = states.CONNECTED
        }
    }
    lastZxid = header.zxid
    println("-->Header reply | XID: " + header.xid + " | ZXID: " + header.zxid + " | ERR: " + header.err)
  }

  def checkWatchEvent(event: WatchEvent) {
    event.state match {
      case 0 => state = states.NOT_CONNECTED
      case 3 => state = states.CONNECTED
      case -112 => state = states.SESSION_EXPIRED
    }
  }

  def reconnect: Future[ConnectResponse] = {
    val recoReq = new ConnectRequest(0, lastZxid, timeOut, sessionId, passwd)
    prepareConnection()
    Try { writer(recoReq) } match {
      case Return(rep) =>
        rep flatMap { connectResponse =>
          completeConnection(connectResponse.asInstanceOf[ConnectResponse])
          // TODO do we need to set watches back if the session has not expired
          Future(connectResponse.asInstanceOf[ConnectResponse])
        }
      case Throw(exc) =>
        // New connection
        connect
    }
  }

  def connect: Future[ConnectResponse] = {
    val recoReq = new ConnectRequest(connectionTimeout = timeOut)
    prepareConnection()
    Try { writer(recoReq) } match {
      case Return(rep) =>
        rep flatMap { connectResponse =>
          // TODO set watches back if it's a reconnection
          completeConnection(connectResponse.asInstanceOf[ConnectResponse])
          Future(connectResponse.asInstanceOf[ConnectResponse])
        }
      case Throw(exc) =>
        state = states.CLOSED
        throw exc
    }
  }
}


class PingTimer {

  val timer = DefaultTimer
  var currentTask: Option[TimerTask] = None
  def apply(period: Duration)(f: => Unit) {
    currentTask = Some(timer.twitter.schedule(period)(f)) }
  //def stopTimer = { timer.twitter.stop() }
  def updateTimer(period: Duration)(f: => Unit) = {
    currentTask.get.cancel()
    apply(period)(f)
  }

}
