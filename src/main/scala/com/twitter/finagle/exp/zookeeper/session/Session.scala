package com.twitter.finagle.exp.zookeeper.session

import com.twitter.conversions.time._
import com.twitter.finagle.exp.zookeeper.{WatchEvent, StateHeader, ZookeeperException}
import com.twitter.finagle.exp.zookeeper.session.Session.States
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.{Duration, TimerTask}
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Represents a ZooKeeper Session
 * @param sessionID ZooKeeper session ID
 * @param sessionPwd ZooKeeper session password
 * @param sessTimeout requested session timeout
 * @param negoTimeout negotiated session timeout
 */
class Session(
  sessionID: Long = 0L,
  sessionPwd: Array[Byte] = Array[Byte](16),
  sessTimeout: Duration = 3000.milliseconds,
  negoTimeout: Duration = 3000.milliseconds,
  canReadOnly: Boolean = false
  ) {
  /**
   * Session variables
   * sessionId - current session Id
   * sessionPassword - current session password
   * sessionTimeout - negotiated session timeout
   * xid - current request Id
   * lastZxid - last ZooKeeper Transaction Id
   */
  var isReadOnly = false
  val sessionId: Long = sessionID
  val sessionPassword: Array[Byte] = sessionPwd
  val sessionTimeout: Duration = sessTimeout
  var negotiatedTimeout: Duration = negoTimeout
  // Ping at 1/3 of timeout, connect to a new host if no response at 2/3 of timeout
  var pingTimeout: Duration = negotiatedTimeout * 1 / 3
  var readTimeout: Duration = negotiatedTimeout * 2 / 3
  @volatile private[this] var xid = 2
  @volatile var lastZxid: Long = 0L
  @volatile var state: States.ConnectionState = States.NOT_CONNECTED

  private[finagle] val isFirstConnect = new AtomicBoolean(true)
  private[finagle] val isClosingSession = new AtomicBoolean(false)
  private[finagle] val pingScheduler = new PingScheduler

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

  /**
   * This is how we start to send ping every x milliseconds
   * @param f a request writer taking a PingRequest as argument
   */
  private[finagle] def startPing(f: => Unit) = pingScheduler(pingTimeout)(f)
  /**
   * Classic way to determine the real interval between two pings
   * @return effective period to send ping
   */

  /**
   * Connect session management :
   */
  def canConnect(): Boolean = {
    if (state == States.NOT_CONNECTED ||
      state == States.CLOSED ||
      state == States.SESSION_EXPIRED ||
      state == States.CONNECTION_LOSS) true
    else {
      throw new ZookeeperException("Connection exception: Session is already established")
    }
  }

  def canClose(): Boolean = {
    if (state != States.CONNECTING &&
      state != States.ASSOCIATING &&
      state != States.CLOSED &&
      state != States.NOT_CONNECTED) true
    else {
      throw new ZookeeperException("Connection exception: Session is not established")
    }
  }

  /**
   * Close session management :
   * -- prepareClose
   * Called before the close session request is written
   *
   * -- close
   * Called after the close session response decoding
   */
  def prepareClose() {
    pingScheduler.currentTask.get.cancel()
    isClosingSession.set(true)
  }
  def close() { state = States.CLOSED }

  /**
   * Here we are parsing the header's error field
   * and changing the connection state if required
   * then the ZXID is updated.
   * @param header a request header•
   */
  def parseStateHeader(header: StateHeader) {
    header.err match {
      case 0 => state = States.CONNECTED // TODO not if readonly manage this
      case -4 => state = States.CONNECTION_LOSS
      case -112 => state = States.SESSION_EXPIRED
      case -118 => state = States.SESSION_MOVED
      case -666 =>
        // -666 is the code for first connect
        if (state == States.CONNECTING) {
          isFirstConnect.set(false)
          state = States.CONNECTED
        }
      case _ =>
    }
    lastZxid = header.zxid
  }

  /**
   * Here we are parsing the watchEvent's state field
   * and changing the connection state if required
   * @param event a request header
   */
  def parseWatchEvent(event: WatchEvent) {
    event.state match {
      case 0 => state = States.NOT_CONNECTED
      case 3 => state = States.CONNECTED
      case -112 => state = States.SESSION_EXPIRED
      case 4 => state = States.AUTH_FAILED
      case 5 =>
        isReadOnly = true
        state = States.CONNECTED_READONLY
      case 6 => state = States.SASL_AUTHENTICATED
      case _ =>
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

object Session {
  /**
   * States describes every possible states of the connection
   */
  object States extends Enumeration {
    type ConnectionState = Value
    val CONNECTING, ASSOCIATING, CONNECTED, CONNECTED_READONLY, CLOSED, AUTH_FAILED, NOT_CONNECTED = Value
    val CONNECTION_LOSS, SESSION_EXPIRED, SESSION_MOVED, SASL_AUTHENTICATED = Value
  }
}