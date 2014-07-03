package com.twitter.finagle.exp.zookeeper.session

import java.util.concurrent.atomic.AtomicBoolean

import com.twitter.finagle.exp.zookeeper.client.ZkClient
import com.twitter.finagle.exp.zookeeper.session.Session.{PingSender, States}
import com.twitter.finagle.exp.zookeeper.{ReplyHeader, ConnectRequest, ConnectResponse, WatchEvent}
import com.twitter.util.{Promise, Duration, Future}

/**
 * Session manager is used to manage sessions during client life
 */
class SessionManager(canBeRo: Boolean) {

  @volatile private[finagle] var session: Promise[Session] = Promise[Session]()

  /**
   * Build a connect request to create a new session
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
   * @return a customized ConnectResponse
   */
  def buildReconnectRequest(): Future[ConnectRequest] = {
    session flatMap { sess =>
      val sessionId = if (sess.hasFakeSessionId.get) 0
      else sess.id

      Future(ConnectRequest(
        0,
        sess.lastZxid.get(),
        sess.diseredTimeout,
        sessionId,
        sess.password,
        canBeRo
      ))
    }
  }

  def canCloseSession: Future[Boolean] = session flatMap (sess => Future(sess.canClose))
  def canCreateSession: Future[Boolean] =
    if (session.isDefined)
      session flatMap (sess => Future(sess.canConnect))
    else Future(true)

  def canReconnect: Future[Boolean] = session flatMap (sess => Future(sess.canConnect))

  /**
   * To close current session and clean session manager
   * @return
   */
  def closeAndClean(): Future[Unit] = {
    session flatMap (sess => Future(sess.close()))
  }

  /**
   * Used to create a fresh new Session from the connect response.
   * Use cases : connection, reconnection with new Session
   * @param conRep connect Response
   * @param sessionTimeout connect request session timeout
   * @param pinger function to send ping request
   * @return Future.Done when new session is configured
   */
  def newSession(
    conRep: ConnectResponse,
    sessionTimeout: Duration,
    pinger: PingSender): Future[Unit] = {
    ZkClient.logger.info(
      "Connected to session with ID: %d".format(conRep.sessionId))
    reset() before {
      session.setValue(new Session(
        conRep.sessionId,
        conRep.passwd,
        sessionTimeout,
        conRep.timeOut,
        new AtomicBoolean(conRep.isRO),
        Some(pinger)))

      session flatMap (sess => Future(sess.init()))
    }
  }

  /**
   * Here we are parsing the header's error field
   * and changing the connection state if required
   * then the ZXID is updated.
   * @param header request's header
   */
  def parseHeader(header: ReplyHeader) {
    session flatMap { sess =>
      header.err match {
        case 0 => // Ok error code
        case -4 => sess.currentState.set(States.CONNECTION_LOSS)
        case -112 => sess.currentState.set(States.SESSION_EXPIRED)
        case -115 => sess.currentState.set(States.AUTH_FAILED)
        case -118 => sess.currentState.set(States.SESSION_MOVED)
        case _ =>
      }
      if (header.zxid > 0) sess.lastZxid.set(header.zxid)
      Future.Done
    }
  }

  /**
   * Here we are parsing the watchEvent's state field
   * and changing the connection state if required
   * @param event a request header
   */
  def parseWatchEvent(event: WatchEvent) {
    session flatMap { sess =>
      event.state match {
        case -112 =>
          sess.stop()
          sess.currentState.set(States.SESSION_EXPIRED)
        case 0 =>
          sess.stop()
          sess.currentState.set(States.NOT_CONNECTED)
        case 3 =>
          sess.isRO.set(false)
          sess.currentState.set(States.CONNECTED)
        case 4 => sess.currentState.set(States.AUTH_FAILED)
        case 5 =>
          sess.isRO.set(true)
          sess.currentState.set(States.CONNECTED_READONLY)
        case 6 => sess.currentState.set(States.SASL_AUTHENTICATED)
        case _ =>
      }
      Future.Done
    }
  }

  /**
   * Used to reconnect with the same session Ids
   * Use cases : session reconnection after connection loss,
   * reconnection to RW mode server.
   * @param conReq connect response
   * @param pinger function to send ping request
   * @return Future.Done when session is configured
   */
  def reconnect(
    conReq: ConnectResponse,
    pinger: PingSender): Future[Unit] = {
    session flatMap { sess =>
      sess.reset() before {
        sess.reinit(conReq, pinger)
        Future.Done
      }
    }
  }

  /**
   * Use reset before reconnection to a server with a new session
   * it will clean up connection and manager.
   * @return Future.Done when session resetting is finished
   */
  private[this] def reset(): Future[Unit] = {
    if (session.isDefined) {
      session flatMap { sess =>
        sess.reset() before {
          sess.hasFakeSessionId.set(true)
          Future.Done
        }
      }
    }
    else Future.Done
  }
}