package com.twitter.finagle.exp.zookeeper.session

import java.util.concurrent.atomic.AtomicBoolean

import com.twitter.finagle.exp.zookeeper.client.ZkClient
import com.twitter.finagle.exp.zookeeper.session.Session.{PingSender, States}
import com.twitter.finagle.exp.zookeeper.{ConnectRequest, ConnectResponse, ReplyHeader, WatchEvent}
import com.twitter.util.{Try, Duration, Future}

/**
 * Session manager is used to manage sessions during client life
 */
class SessionManager(canBeRo: Boolean) {

  @volatile private[finagle] var session: Session = new Session()

  /**
   * Build a connect request to create a new session
   *
   * @return a customized ConnectResponse
   */
  def buildConnectRequest(sessionTimeout: Duration): ConnectRequest = {

    ConnectRequest(
      0,
      0L,
      sessionTimeout,
      0L,
      Array[Byte](16),
      canBeRo
    )
  }

  /**
   * Build a reconnect request depending if RO mode is allowed by user,
   * and if current session has a fake session ID ( never connected
   * to RW server)
   *
   * @return a customized ConnectResponse
   */
  def buildReconnectRequest(): ConnectRequest = {
    val sessionId = if (session.hasFakeSessionId.get) 0
    else session.id

    ConnectRequest(
      0,
      session.lastZxid.get(),
      session.diseredTimeout,
      sessionId,
      session.password,
      canBeRo
    )
  }

  def canCloseSession: Boolean = session.canClose
  def canCreateSession: Boolean = session.canConnect
  def canReconnect: Boolean = session.canConnect

  /**
   * To close current session and clean session manager
   */
  def closeAndClean() {
    session.close()
  }

  /**
   * Used to create a fresh new Session from the connect response.
   * Use cases : connection, reconnection with new Session
   *
   * @param conRep connect Response
   * @param sessionTimeout connect request session timeout
   * @param pinger function to send ping request
   * @return Future.Done when new session is configured
   */
  def newSession(
    conRep: ConnectResponse,
    sessionTimeout: Duration,
    pinger: PingSender) {
    ZkClient.logger.info(
      "Connected to session with ID: %d".format(conRep.sessionId))

    reset()
    session = new Session(
      conRep.sessionId,
      conRep.passwd,
      sessionTimeout,
      conRep.timeOut,
      new AtomicBoolean(conRep.isRO),
      Some(pinger))

    session.init()
  }

  /**
   * Here we are parsing the header's error field
   * and changing the connection state if required
   * then the ZXID is updated.
   *
   * @param header request's header
   */
  def parseHeader(header: ReplyHeader) {
    header.err match {
      case 0 => // Ok error code
      case -4 =>
        session.currentState.set(States.CONNECTION_LOSS)
        ZkClient.logger.warning("Received CONNECTION_LOSS event from server")
      case -112 =>
        session.currentState.set(States.SESSION_EXPIRED)
        ZkClient.logger.warning("Session %d has expired".format(session.id))
      case -115 =>
        session.currentState.set(States.AUTH_FAILED)
        ZkClient.logger.warning("Authentication to server has failed")
      case -118 => session.currentState.set(States.SESSION_MOVED)
        ZkClient.logger.warning("Session has moved to another server")
      case _ =>
    }
    if (header.zxid > 0) session.lastZxid.set(header.zxid)
  }

  /**
   * Here we are parsing the watchEvent's state field
   * and changing the connection state if required
   *
   * @param event a request header
   */
  def parseWatchEvent(event: WatchEvent) {
    event.state match {
      case -112 =>
        session.stop()
        session.currentState.set(States.SESSION_EXPIRED)
        ZkClient.logger.warning("Session %d has expired".format(session.id))
      case 0 =>
        session.stop()
        session.currentState.set(States.NOT_CONNECTED)
        ZkClient.logger.warning("Received NOT_CONNECTED event from server")
      case 3 =>
        session.isRO.set(false)
        session.hasFakeSessionId.set(false)
        session.currentState.set(States.CONNECTED)
        ZkClient.logger.info("Server is now in Read-Write mode")
      case 4 => session.currentState.set(States.AUTH_FAILED)
      case 5 =>
        session.isRO.set(true)
        session.currentState.set(States.CONNECTED_READONLY)
        ZkClient.logger.info("Server is now in Read Only mode")
      case 6 =>
        session.currentState.set(States.SASL_AUTHENTICATED)
        ZkClient.logger.info("SASL authentication confirmed by server")
      case _ =>
    }
  }

  /**
   * Used to reconnect with the same session Ids
   * Use cases : session reconnection after connection loss,
   * reconnection to RW mode server.
   *
   * @param conReq connect response
   * @param pinger function to send ping request
   * @return Future.Done when session is configured
   */
  def reinit(
    conReq: ConnectResponse,
    pinger: PingSender): Try[Unit] = {
    session.reset()
    session.reinit(conReq, pinger)
  }

  /**
   * Use reset before reconnection to a server with a new session
   * it will clean up connection and manager.
   *
   * @return Future.Done when session resetting is finished
   */
  private[this] def reset() {
    session.reset()
    session.hasFakeSessionId.set(true)
  }
}