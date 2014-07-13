package com.twitter.finagle.exp.zookeeper.client.managers

import com.twitter.finagle.exp.zookeeper._
import com.twitter.finagle.exp.zookeeper.client.ZkClient
import com.twitter.finagle.exp.zookeeper.client.managers.ClientManager.{CantReconnect, ConnectionFailed}
import com.twitter.finagle.exp.zookeeper.connection.Connection.NoConnectionAvailable
import com.twitter.finagle.exp.zookeeper.session.Session.{NoSessionEstablished, SessionAlreadyEstablished, States}
import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.OpCode
import com.twitter.util.{Future, Return, Throw}
import com.twitter.util.TimeConversions._

trait ClientManager extends AutoLinkManager
with ReadOnlyManager {slf: ZkClient =>

  type ConnectResponseBehaviour = ConnectResponse => Future[Unit]

  /**
   * Should take decisions depending on canBeRO, isRO and if timeBetweenRwSrch
   * is defined.
   *
   */
  private[this] def actIfRO(): Unit = {
    if (sessionManager.session.isRO.get()
      && timeBetweenRwSrch.isDefined) {
      startRwSearch()
    }
  }

  /**
   * It should make decisions depending on the connect response and
   * session status
   *
   * @param conRep the connect response
   * @return Future.Done or Exception
   */
  private[this] def actOnConnect(conRep: ConnectResponse): Future[Unit] = {
    if (conRep.timeOut <= 0.milliseconds) {
      sessionManager.session.currentState.set(States.CLOSED)
      Future.exception(new ConnectionFailed("Session timeout <= 0"))
    } else {
      sessionManager.newSession(conRep, sessionTimeout, ping)
      configureNewSession() before startJob()
    }
  }

  /**
   * It should make decisions depending on tries counter, connect response,
   * and session status
   *
   * @param conRep the connect response
   * @param tries current number of tries
   * @return Future.Done or exception
   */
  private[this] def actOnReconnectWithSession(
    host: Option[String],
    tries: Int)(conRep: ConnectResponse): Future[Unit] = {

    if (conRep.timeOut <= 0.milliseconds) {
      sessionManager.session.currentState.set(States.NOT_CONNECTED)
      reconnectWithoutSession(host, tries + 1)
    } else {
      val seenRwBefore = !sessionManager.session.hasFakeSessionId.get()
      if (seenRwBefore) {
        if (conRep.isRO) {
          sessionManager.reinit(conRep, ping) rescue {
            case exc: Throwable =>
              Return(sessionManager.newSession(conRep, sessionTimeout, ping))
          }
          configureNewSession() before startJob()
        }
        else {
          sessionManager.reinit(conRep, ping) match {
            case Return(unit) =>
              startStateLoop()
              Future(zkRequestService.unlockService())
            case Throw(exc) =>
              sessionManager
                .newSession(conRep, sessionTimeout, ping)
              configureNewSession() before startJob()
          }
        }
      }
      else actOnReconnectWithoutSession(host, tries)(conRep)
    }
  }

  /**
   * Should take a decision depending on connect response
   *
   * @param host a host to reconnect to
   * @param tries number of tries
   * @param conRep connect response
   * @return Future.Done or Exception
   */
  private[this] def actOnReconnectWithoutSession(
    host: Option[String],
    tries: Int)
    (conRep: ConnectResponse): Future[Unit] = {
    if (conRep.timeOut <= 0.milliseconds) {
      sessionManager.session.currentState.set(States.NOT_CONNECTED)
      reconnectWithoutSession(host, tries + 1)
    } else {
      sessionManager.newSession(conRep, sessionTimeout, ping)
      configureNewSession()
      startJob()
    }

  }

  /**
   * A composable connect operation, could be used to create a new session
   * or reconnect.
   *
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

  /**
   * Should perform an action after connecting to a server.
   *
   * @param connectReq a connect request to send
   * @param actOnEvent a decision to make after receiving to connect response
   * @return Future.Done or Exception
   */
  private[this] def onConnect(
    connectReq: => ConnectRequest,
    actOnEvent: ConnectResponseBehaviour
    ): Future[Unit] = {
    sessionManager.session.currentState.set(States.CONNECTING)
    configureDispatcher() before
      connectionManager.connection.get.serve(
        ReqPacket(None, Some(connectReq))
      ) transform {
      case Return(connectResponse) =>
        val finalRep = connectResponse.response.get.asInstanceOf[ConnectResponse]
        actOnEvent(finalRep)

      case Throw(exc: Throwable) =>
        sessionManager.session.currentState.set(States.NOT_CONNECTED)
        Future.exception(exc)
    }
  }

  /**
   * Should try to connect to a new host if possible.
   * This operation will interrupt request sending until a new
   * connection and a new session are established
   *
   * @param host the server to connect to
   * @return Future.Done or Exception
   */
  def changeHost(host: Option[String] = None): Future[Unit] =
    if (host.isDefined)
      connectionManager.hostProvider.testOrFind(host.get) flatMap { server =>
        reconnectWithSession(Some(server))
      }
    else {
      connectionManager.hostProvider.seenRwServer match {
        case Some(rwServer) =>
          if (rwServer == connectionManager.currentHost.get)
            connectionManager.hostProvider.seenRwServer = None
        case None =>
      }

      val newList = connectionManager.hostProvider.serverList
        .filter(connectionManager.currentHost.getOrElse("") == _)
      connectionManager.hostProvider.findServer(Some(newList)).flatMap { srv =>
        connectionManager.close() before
          reconnectWithSession(Some(srv))
      }
    }

  /**
   * Should add some new servers to the host list
   *
   * @param hostList hosts to add
   */
  def addHosts(hostList: String): Unit = connectionManager.addHosts(hostList)

  /**
   * Should remove some hosts from the host list
   *
   * @param hostList hosts to remove
   * @return Future.Done or Exception
   */
  def removeHosts(hostList: String): Future[Unit] = {
    connectionManager.removeAndFind(hostList) flatMap { server =>
      connectionManager.currentHost match {
        case Some(host) =>
          if (host == server)
            Future(connectionManager.hostProvider.removeHost(hostList))
          else
            reconnectWithSession(Some(server)) before
              Future(connectionManager.hostProvider.removeHost(hostList))
        case None =>
            Future(connectionManager.hostProvider.removeHost(hostList))
      }
    }
  }

  /**
   * Should close the current session, however it should not
   * close the service and factory.
   *
   * @return Future.Done or Exception
   */
  def disconnect(): Future[Unit] =
    if (sessionManager.canCloseSession) {
      stopJob() before {
        connectionManager.hasAvailableConnection flatMap { available =>
          if (available) {
            sessionManager.session.isClosingSession.set(true)
            val closeReq = ReqPacket(Some(RequestHeader(1, OpCode.CLOSE_SESSION)), None)

            connectionManager.connection.get.serve(closeReq) transform {
              case Return(closeRep) =>
                if (closeRep.err.get == 0) {
                  watchManager.clearWatchers()
                  sessionManager.session.close()
                  Future.Done
                } else Future.exception(
                  ZookeeperException.create("Error while closing session", closeRep.err.get))

              case Throw(exc: Throwable) => Future.exception(exc)
            }
          } else Future.exception(
            new NoConnectionAvailable("connection not available during close session"))
        }
      }
    } else Future.exception(
      new NoSessionEstablished("client is not connected to server")) ensure stopJob()

  /**
   * Should find a suitable server and connect to.
   * This operation will interrupt request sending until a new
   * connection and a new session are established
   *
   * @return Future.Done or Exception
   */
  def newSession(host: Option[String]): Future[Unit] =
    if (sessionManager.canCreateSession) {
      val connectReq = sessionManager.buildConnectRequest(sessionTimeout)
      stopJob() before connect(connectReq, host, actOnConnect)
    } else Future.exception(
      new SessionAlreadyEstablished(
        "client is already connected to server")) ensure stopJob()


  /**
   * Should reconnect to a suitable server and try to regain the session,
   * or else create a new session.
   *
   * @return Future.Done or Exception
   */
  private[this] def reconnect(
    host: Option[String] = None,
    tries: Int = 0,
    requestBuilder: => ConnectRequest,
    actOnEvent: (Option[String], Int) => ConnectResponseBehaviour
    ): Future[Unit] =
    if (sessionManager.canReconnect && tries < maxConsecutiveRetries.get) {
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


  /**
   * Should reconnect to a host using the last session ids.
   *
   * @param host a server to reconnect to
   * @param tries tries counter
   * @return Future.Done or Exception
   */
  private[finagle] def reconnectWithSession(
    host: Option[String] = None,
    tries: Int = 0
    ): Future[Unit] =

    reconnect(
      host,
      tries,
      sessionManager.buildReconnectRequest(),
      actOnReconnectWithSession)

  /**
   * Should reconnect without using the last session ids.
   *
   * @param host a server to reconnect to
   * @param tries tries counter
   * @return Future.Done or Exception
   */
  private[finagle] def reconnectWithoutSession(
    host: Option[String] = None,
    tries: Int = 0
    ): Future[Unit] =

    reconnect(
      host,
      tries,
      sessionManager.buildConnectRequest(sessionTimeout),
      actOnReconnectWithoutSession)

  /**
   * Should start connection and session management jobs :
   * link checker's loop, rw mode server search, preventive search,
   * and unlock the zookeeper request service.
   *
   * @return Future.Done or Exception
   */
  private[finagle] def startJob(): Future[Unit] = {
    startStateLoop()
    actIfRO()
    connectionManager.hostProvider.startPreventiveSearch()
    Future(zkRequestService.unlockService())
  }

  /**
   * Should stop background jobs like preventive search.
   *
   * @return Future.Done
   */
  private[finagle] def stopJob(): Future[Unit] =
    zkRequestService.lockService() before {
      stopStateLoop()
      stopRwSearch before
        connectionManager.hostProvider.stopPreventiveSearch
    }

  /**
   * Should configure the dispatcher and session after reconnection
   * with new session.
   *
   * @return Future.Done
   */
  private[finagle] def configureNewSession(): Future[Unit] =
    setWatches() before recoverAuth()

}

private[finagle] object ClientManager {
  class ConnectionFailed(msg: String) extends RuntimeException(msg)
  class CantReconnect(msg: String) extends RuntimeException(msg)
}