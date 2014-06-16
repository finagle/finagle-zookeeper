package com.twitter.finagle.exp.zookeeper.session

import com.twitter.conversions.time._
import com.twitter.finagle.exp.zookeeper.{StateHeader, ZookeeperException}
import com.twitter.finagle.exp.zookeeper.session.Session.States
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.{Duration, TimerTask}
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Une session represente un accès à un serveur zookeeper
 * Il définit l'activité de la liaison établie
 */

class Session(
  sessionID: Long = 0L
  , sessionPwd: Array[Byte] = Array[Byte](16)
  , timeout: Int = 3000) {
  /**
   * Session variables
   * sessionId - current session Id
   * sessionPassword - current session password
   * sessionTimeout - negotiated session timeout
   * xid - current request Id
   * lastZxid - last ZooKeeper Transaction Id
   */
  /*
    private[this] val sessionId: Long = sessionID
    private[this] val sessionPassword: Array[Byte] = Array[Byte](16)
    private[this] val sessionTimeOut: Int = timeout*/
  @volatile private[this] var xid = 2
  @volatile private[this] var lastZxid: Long = 0L
  @volatile var state: States.ConnectionState = States.NOT_CONNECTED

  private[finagle] var isFirstConnect: Boolean = true
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
  private[finagle] def startPing(f: => Unit) = pingScheduler(realTimeout.milliseconds)(f)
  /**
   * Classic way to determine the real interval between two pings
   * @return effective period to send ping
   */
  private[this] def realTimeout: Long = timeout * 2 / 3

  /**
   * Connect session management :
   */
  def canConnect {
    if (state == States.NOT_CONNECTED ||
      state == States.CLOSED ||
      state == States.SESSION_EXPIRED ||
      state == States.CONNECTION_LOST) true
    else {
      throw new ZookeeperException("Connection exception: Session is already established")
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
    val CONNECTION_LOST, SESSION_EXPIRED, SESSION_MOVED, SASL_AUTHENTICATED = Value
  }
}