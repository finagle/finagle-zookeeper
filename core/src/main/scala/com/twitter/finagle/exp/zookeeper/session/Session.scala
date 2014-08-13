package com.twitter.finagle.exp.zookeeper.session

import com.twitter.finagle.exp.zookeeper._
import com.twitter.finagle.exp.zookeeper.client.ZkClient
import com.twitter.finagle.exp.zookeeper.session.Session._
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.{Try, Duration, Future, TimerTask}
import java.util
import java.util.concurrent.atomic.{AtomicReference, AtomicLong, AtomicInteger, AtomicBoolean}
import com.twitter.util.TimeConversions._


/**
 * A Session contains ZooKeeper Session Ids and is in charge of sending
 * ping requests depending on negotiated session timeout
 *
 * @param sessionID ZooKeeper session ID
 * @param sessionPassword ZooKeeper session password
 * @param sessionTimeout requested session timeout
 * @param negotiateTimeout negotiated session timeout
 * @param isRO if the session is currently read only
 */
class Session(
  sessionID: Long = 0L,
  sessionPassword: Array[Byte] = Array[Byte](16),
  sessionTimeout: Duration = 0.milliseconds,
  var negotiateTimeout: Duration = 0.milliseconds,
  var isRO: AtomicBoolean = new AtomicBoolean(false),
  var pingSender: Option[PingSender] = None
) {

  private[finagle]
  var currentState =
    new AtomicReference[States.ConnectionState](States.NOT_CONNECTED)
  private[finagle] val isClosingSession = new AtomicBoolean(false)
  private[finagle] var hasFakeSessionId = new AtomicBoolean(true)
  private[finagle] val lastZxid = new AtomicLong(0L)
  private[this] val xid = new AtomicInteger(2)

  def isReadOnly: Boolean = this.isRO.get()
  def id: Long = sessionID
  def password: Array[Byte] = sessionPassword

  /**
   * Ping every 1/3 of timeout, connect to a new host
   * if no response at (lastRequestTime) + 2/3 of timeout
   */
  private[this] def pingTimeout: Duration = negotiateTimeout * 1 / 3
  def diseredTimeout: Duration = sessionTimeout
  def negotiatedTimeout: Duration = negotiateTimeout
  def state: States.ConnectionState = currentState.get()

  /**
   * Determines if we can use init or reinit
   *
   * @return true or exception
   */
  private[finagle] def canConnect: Boolean = currentState.get() match {
    case States.NOT_CONNECTED |
         States.CLOSED |
         States.SESSION_EXPIRED |
         States.SESSION_MOVED |
         States.CONNECTION_LOSS => true
    case _ => false
  }

  /**
   * Determines if we can close the session
   *
   * @return true or exception
   */
  private[finagle] def canClose: Boolean = currentState.get() match {
    case States.CONNECTING |
         States.ASSOCIATING |
         States.CLOSED |
         States.NOT_CONNECTED => false
    case _ => true
  }

  /**
   * Determines if we can reconnect to a server.
   * @return whether or not we can reconnect
   */
  private[finagle] def canReconnect: Boolean = currentState.get() match {
    case States.CONNECTING |
         States.ASSOCIATING => false
    case _ => true
  }

  /**
   * Called after the close session response decoding
   */
  private[finagle] def close() {
    isClosingSession.set(false)
    currentState.set(States.CLOSED)
  }

  /**
   * To get the next xid
   * @return the next unique XID
   */
  private[finagle] def nextXid: Int = xid.getAndIncrement

  /**
   * Use init just after session creation
   */
  private[finagle] def init() {
    if (!PingScheduler.isRunning) {
      if (!pingSender.isDefined)
        throw new RuntimeException("Ping sender not defined in Session")
      xid.set(2)
      lastZxid.set(0L)
      startPing()
      isClosingSession.set(false)
      if (isRO.get()) {
        currentState.set(States.CONNECTED_READONLY)
        ZkClient.logger.info("Server is in Read Only mode")
      }
      else {
        currentState.set(States.CONNECTED)
        ZkClient.logger.info("Server is in Read-Write mode")
        hasFakeSessionId.set(false)
      }
    } else throw new RuntimeException(
      "Pinger is not initiated or PingScheduler is already running in Session")
  }

  /**
   * Called before the close session request is written
   */
  private[finagle] def prepareClose() {
    stopPing()
    isClosingSession.set(true)
  }

  /**
   * Reinitialize session with a connect response and a function sending ping
   *
   * @param connectResponse a connect response
   * @param pingSender a function sending ping and receiving response
   */
  private[finagle] def reinit(
    connectResponse: ConnectResponse,
    pingSender: PingSender
  ): Try[Unit] = Try {
    assert(connectResponse.sessionId == sessionID)
    assert(util.Arrays.equals(connectResponse.passwd, password))

    stopPing()
    isClosingSession.set(false)
    isRO.set(connectResponse.isRO)
    if (isRO.get()) {
      currentState.set(States.CONNECTED_READONLY)
      ZkClient.logger.info("Server is in Read Only mode")
    }
    else {
      currentState.set(States.CONNECTED)
      ZkClient.logger.info("Server is in Read-Write mode")
      hasFakeSessionId.set(false)
    }
    this.pingSender = Some(pingSender)
    negotiateTimeout = connectResponse.timeOut
    startPing()
    xid.set(2)
    ZkClient.logger.info(
      s"Reconnected to session with ID: ${connectResponse.sessionId}")
  }

  private[finagle] def stop() {
    stopPing()
  }

  /**
   * This is how we send ping every x milliseconds
   */
  private[this]
  def startPing(): Unit = PingScheduler(pingTimeout)(pingSender.get())

  private[this]
  def stopPing(): Unit = PingScheduler.stop()


  /**
   * PingScheduler is used to send Ping Request to the server
   * every x milliseconds, if the connection is alive. If we issue
   * a connection loss then the current task is cancelled and a new one
   * is created after the reconnection succeeded.
   */
  private[finagle] object PingScheduler {

    /**
     * currentTask - the last scheduled timer's task
     */
    var currentTask: Option[TimerTask] = None

    def apply(period: Duration)(f: => Unit) {
      currentTask = Some(DefaultTimer.twitter.schedule(period)(f))
    }

    def isRunning: Boolean = {
      currentTask.isDefined
    }

    def stop() {
      if (currentTask.isDefined) currentTask.get.cancel()
      currentTask = None
    }

    def updateTimer(period: Duration)(f: => Unit) = {
      stop()
      apply(period)(f)
    }
  }

}

object Session {
  type PingSender = () => Future[Unit]

  class SessionAlreadyEstablished(msg: String) extends RuntimeException(msg)

  class NoSessionEstablished(msg: String) extends RuntimeException(msg)

  /**
   * States describes every possible states of the connection
   */
  object States extends Enumeration {
    type ConnectionState = Value
    val CONNECTING, ASSOCIATING, CONNECTED, CONNECTED_READONLY,
    CLOSED, AUTH_FAILED, NOT_CONNECTED, CONNECTION_LOSS, SESSION_EXPIRED,
    SESSION_MOVED, SASL_AUTHENTICATED = Value
  }
}