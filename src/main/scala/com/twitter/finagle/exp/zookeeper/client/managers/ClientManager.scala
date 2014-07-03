package com.twitter.finagle.exp.zookeeper.client.managers

import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.OpCode
import com.twitter.finagle.exp.zookeeper._
import com.twitter.finagle.exp.zookeeper.client.ZkClient
import com.twitter.finagle.exp.zookeeper.client.managers.ClientManager.{CantReconnect, ConnectionFailed}
import com.twitter.finagle.exp.zookeeper.connection.Connection.NoConnectionAvailable
import com.twitter.finagle.exp.zookeeper.session.Session.{NoSessionEstablished, SessionAlreadyEstablished, States}
import com.twitter.util.{Duration, Future, Return, Throw}

trait ClientManager extends AutoLinkManager with ReadOnlyMode {slf: ZkClient =>


  type ConnectResponseBehaviour = ConnectResponse => Future[Unit]

  /**
   * Should take decisions depending on canBeRO, isRO and if timeBetweenRwSrch
   * is defined.
   * @return Future.Done
   */
  def actIfRO(): Future[Unit] = {
    sessionManager.session flatMap { sess =>
      if (sess.isReadOnly.get()
        && timeBetweenRwSrch.isDefined) {
        startRwSearch()
        Future.Done
      } else Future.Done
    }
  }

  /**
   * It should make decisions depending on the connect response and
   * session status
   * @param conRep the connect response
   * @return Future.Done
   */
  def actOnConnect(conRep: ConnectResponse): Future[Unit] = {
    if (conRep.timeOut <= Duration.Bottom) {
      if (sessionManager.session.isDefined) {
        sessionManager.session flatMap { sess =>
          Future(sess.currentState.set(States.NOT_CONNECTED))
        }
      }
      Future.exception(new ConnectionFailed("Session timeout <= 0"))
    } else {
      sessionManager.newSession(conRep, sessionTimeout, ping) before
        configureNewSession() before startJob()
    }
  }

  /**
   * It should make decisions depending on tries counter, connect response,
   * and session status
   * @param conRep the connect response
   * @param tries current number of tries
   * @return Future.Done or exception
   */
  def actOnReconnectWithSession(
    host: Option[String],
    tries: Int)(conRep: ConnectResponse): Future[Unit] = {
    sessionManager.session flatMap { sess =>
      if (conRep.timeOut <= Duration.Bottom) {
        sess.currentState.set(States.NOT_CONNECTED)
        reconnectWithoutSession(host, tries + 1)
      } else {
        val seenRwBefore = !sess.hasFakeSessionId.get()
        if (seenRwBefore) {
          if (conRep.isRO) {
            // todo log connection to ro server after rw one
            sessionManager.reconnect(conRep, ping) rescue {
              case exc: Exception =>
                sessionManager.newSession(conRep, sessionTimeout, ping)
            } before configureNewSession() before startJob()
          } else {
            // todo log reconnection to a majority server with same session
            sessionManager.reconnect(conRep, ping) transform {
              case Return(unit) => startStateLoop() before
                zkRequestService.unlockServe()
              case Throw(exc) =>
                sessionManager
                  .newSession(conRep, sessionTimeout, ping) before
                  configureNewSession() before startJob()
            }
          }
        } else actOnReconnectWithoutSession(host, tries)(conRep)
      }
    }
  }

  def actOnReconnectWithoutSession(
    host: Option[String],
    tries: Int)
    (conRep: ConnectResponse): Future[Unit] = {
    sessionManager.session flatMap { sess =>
      if (conRep.timeOut <= Duration.Bottom) {
        sess.currentState.set(States.NOT_CONNECTED)
        reconnectWithoutSession(host, tries + 1)
      } else {
        // todo log if we are in ro mode
        sessionManager.newSession(conRep, sessionTimeout, ping) before
          configureNewSession() before startJob()
      }
    }
  }

  /**
   * A composable connect operation, could be used to create a new session
   * or reconnect.
   * @param connectReq the connect request to use
   * @param actOnEvent the decision to make with connect response
   * @return Future.Done or exception
   */
  private[this] def connect(
    connectReq: => ConnectRequest,
    host: Option[String],
    actOnEvent: ConnectResponseBehaviour
    ): Future[Unit] =
    if (host.isDefined) connectionManager.testAndConnect(host.get) transform {
      case Return(unit) => onConnect(connectReq, actOnEvent)
      case Throw(exc) => connect(connectReq, None, actOnEvent)
    }
    else connectionManager.hasAvailableConnection flatMap { available =>
      if (available) onConnect(connectReq, actOnEvent)
      else connectionManager.findAndConnect() before
        connect(connectReq, host, actOnEvent)
    }


  def onConnect(
    connectReq: => ConnectRequest,
    actOnEvent: ConnectResponseBehaviour): Future[Unit] = {
    if (sessionManager.session.isDefined) {
      sessionManager.session flatMap { sess =>
        Future(sess.currentState.set(States.CONNECTING))
      }
    }
    configureDispatcher() before
      connectionManager.connection flatMap {
      _.serve(
        ReqPacket(None, Some(connectReq))
      ) transform {
        case Return(connectResponse) =>
          val rep = connectResponse.response.get.asInstanceOf[ConnectResponse]
          actOnEvent(rep)

        case Throw(exc: Throwable) =>
          if (sessionManager.session.isDefined) {
            sessionManager.session flatMap { sess =>
              Future(sess.currentState.set(States.NOT_CONNECTED))
            }
          }
          Future.exception(exc)
      }
    }
  }


  /**
   * Should try to connect to a new host if possible.
   * This operation will interrupt request sending until a new
   * connection and a new session are established
   * @param host the server to connect to
   * @return FutureDone or NoServerFound exception
   */
  def changeHost(host: Option[String]): Future[Unit] =
    if (host.isDefined)
      connectionManager.hostProvider.testOrFind(host.get) flatMap { server =>
        reconnectWithSession(Some(server))
      }
    else {
      if (connectionManager.hostProvider.seenRwServer.isDefined
        && connectionManager.hostProvider.seenRwServer.get == host.get)
        connectionManager.hostProvider.seenRwServer = None
      val newList = connectionManager.hostProvider.serverList
        .filter(connectionManager.currentHost.getOrElse("") == _)
      connectionManager.hostProvider.findServer(Some(newList))
        .flatMap { server =>
        reconnectWithSession(Some(server))
      }
    }

  def addHosts(hostList: String) = connectionManager.addHosts(hostList)

  def removeHosts(hostList: String) = {
    connectionManager.removeHosts(hostList) flatMap { server =>
      if (connectionManager.currentHost.isDefined
        && connectionManager.currentHost.get == server)
        Future(connectionManager.hostProvider.removeHost(hostList))
      else
        reconnectWithSession(Some(server)) before
          Future(connectionManager.hostProvider.removeHost(hostList))
    }
  }

  /**
   * Should close the current session, however it should not
   * close the service and factory.
   * @return Future.Done
   */
  def disconnect(): Future[Unit] =
    sessionManager.canCloseSession flatMap {
      if (_) {
        stopJob() before {
          connectionManager.hasAvailableConnection flatMap { available =>
            if (available) {
              val closeReq = ReqPacket(
                Some(RequestHeader(1, OpCode.CLOSE_SESSION)),
                None)
              connectionManager.connection flatMap {
                _.serve(closeReq) transform {
                  case Return(closeRep) =>
                    if (closeRep.err.get == 0) {
                      watchManager.clearWatches()
                      sessionManager.closeAndClean()
                    } else Future.exception(
                      ZookeeperException.create(
                        "Error while closing session", closeRep.err.get))

                  case Throw(exc: Throwable) => Future.exception(exc)
                }
              }
            } else Future.exception(
              new NoConnectionAvailable(
                "connection not available during close session"))
          }
        }
      } else Future.exception(
        new NoSessionEstablished(
          "client is not connected to server")) ensure stopJob()
    }

  /**
   * Should find a suitable server and connect to.
   * This operation will interrupt request sending until a new
   * connection and a new session are established
   * @return Future.Done or NoServerFound exception
   */
  def newSession(host: Option[String]): Future[Unit] =
    sessionManager.canCreateSession flatMap {
      if (_) {
        val connectReq = sessionManager.buildConnectRequest(sessionTimeout)
        stopJob() before connect(connectReq, host, actOnConnect)
      } else Future.exception(
        new SessionAlreadyEstablished(
          "client is already connected to server")) ensure stopJob()
    }

  /**
   * Should reconnect to a suitable server and try to regain the session,
   * or else create a new session.
   * @return Future.Done or NoServerFound exception
   */
  def reconnect(
    host: Option[String] = None,
    tries: Int = 0,
    requestBuilder: => ConnectRequest,
    actOnEvent: (Option[String], Int) => ConnectResponseBehaviour
    ): Future[Unit] =
    sessionManager.canReconnect flatMap { canReco =>
      if (canReco && tries < maxConsecutiveRetries.get) {
        stopJob() before connect(
          requestBuilder,
          host,
          actOnEvent(host, tries)
        ).rescue {
          case exc: Throwable =>
            reconnect(host, tries + 1, requestBuilder, actOnEvent)
        }
      } else Future.exception(
        new CantReconnect("Reconnection not allowed")) ensure stopJob()
    }

  def reconnectWithSession(
    host: Option[String] = None,
    tries: Int = 0
    ): Future[Unit] =

    sessionManager.buildReconnectRequest flatMap {
      reconnect(
        host,
        tries,
        _,
        actOnReconnectWithSession)
    }


  def reconnectWithoutSession(
    host: Option[String] = None,
    tries: Int = 0
    ): Future[Unit] =

    reconnect(
      host,
      tries,
      sessionManager.buildConnectRequest(sessionTimeout),
      actOnReconnectWithoutSession)

  private[finagle] def startJob(): Future[Unit] = {
    startStateLoop() before actIfRO() before
      Future(connectionManager.hostProvider.startPreventiveSearch()) before
      zkRequestService.unlockServe()
  }

  /**
   * Should stop background jobs like preventive search
   * @return Future.Done
   */
  private[finagle] def stopJob(): Future[Unit] =
    zkRequestService.lockServe() before
      stopStateLoop() before stopRwSearch before
      connectionManager.hostProvider.stopPreventiveSearch

  /**
   * Should configure the dispatcher and session after reconnection
   * with new session
   * @return Future.Done
   */
  def configureNewSession(): Future[Unit] =
    setWatches() before recoverAuth()

}

private[finagle] object ClientManager {
  class ConnectionFailed(msg: String) extends RuntimeException(msg)
  class CantReconnect(msg: String) extends RuntimeException(msg)
}