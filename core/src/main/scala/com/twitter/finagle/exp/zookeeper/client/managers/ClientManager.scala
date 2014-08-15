package com.twitter.finagle.exp.zookeeper.client.managers

import com.twitter.finagle.exp.zookeeper._
import com.twitter.finagle.exp.zookeeper.client.ZkClient
import com.twitter.finagle.exp.zookeeper.client.managers.ClientManager.{CantReconnect, ConnectionFailed}
import com.twitter.finagle.exp.zookeeper.connection.HostUtilities
import com.twitter.finagle.exp.zookeeper.session.Session.{SessionAlreadyEstablished, States}
import com.twitter.util.TimeConversions._
import com.twitter.util.{Future, Return, Throw}

trait ClientManager extends AutoLinkManager
with ReadOnlyManager {slf: ZkClient =>

  type ConnectResponseBehaviour = ConnectResponse => Future[String]

  /**
   * Should take decisions depending on canBeRO, isRO and if timeBetweenRwSrch
   * is defined.
   *
   */
  private[this] def actIfRO(): Future[Unit] = {
    if (sessionManager.session.isRO.get()
      && timeBetweenRwSrch.isDefined) {
      startRwSearch()
    }
    else Future.Done
  }

  /**
   * It should make decisions depending on the connect response and
   * session status
   *
   * @param conRep the connect response
   * @return the currently connected server IP or Exception
   */
  private[this] def actOnConnect(conRep: ConnectResponse): Future[String] = {
    if (conRep.timeOut <= 0.milliseconds) {
      sessionManager.session.currentState.set(States.CLOSED)
      Future.exception(new ConnectionFailed("Session timeout <= 0"))
    }
    else {
      sessionManager.newSession(conRep, sessionTimeout, ping)
      configureNewSession() before startJob() before
        Future(connectionManager.currentHost.getOrElse(""))
    }
  }

  /**
   * It should make decisions depending on tries counter, connect response,
   * and session status
   *
   * @param conRep the connect response
   * @param tries current number of tries
   * @return the currently connected server IP or exception
   */
  private[this] def actOnReconnectWithSession(
    host: Option[String],
    tries: Int)(conRep: ConnectResponse): Future[String] = {

    if (conRep.timeOut <= 0.milliseconds) {
      sessionManager.session.currentState.set(States.NOT_CONNECTED)
      reconnectWithoutSession(host, tries + 1)
    }
    else {
      val seenRwBefore = !sessionManager.session.hasFakeSessionId.get()
      if (seenRwBefore) {
        if (conRep.isRO) {
          sessionManager.reinit(conRep, ping) rescue {
            case exc: Throwable =>
              Return(sessionManager.newSession(conRep, sessionTimeout, ping))
          }
          configureNewSession() before startJob() before
            Future(host.getOrElse(""))
        }
        else {
          sessionManager.reinit(conRep, ping) match {
            case Return(unit) => startJob() before Future(host.getOrElse(""))
            case Throw(exc) =>
              sessionManager
                .newSession(conRep, sessionTimeout, ping)
              configureNewSession() before startJob() before
                Future(host.getOrElse(""))
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
   * @return the currently connected server IP or Exception
   */
  private[this] def actOnReconnectWithoutSession(
    host: Option[String],
    tries: Int)
      (conRep: ConnectResponse): Future[String] = {
    if (conRep.timeOut <= 0.milliseconds) {
      sessionManager.session.currentState.set(States.NOT_CONNECTED)
      reconnectWithoutSession(host, tries + 1)
    }
    else {
      sessionManager.newSession(conRep, sessionTimeout, ping)
      configureNewSession() before startJob() before
        Future(host.getOrElse(""))
    }

  }

  /**
   * A composable connect operation, could be used to create a new session
   * or reconnect.
   *
   * @param connectReq the connect request to use
   * @param actOnEvent the decision to make with connect response
   * @return the currently connected server IP
   */
  private[this] def connect(
    connectReq: => ConnectRequest,
    host: Option[String],
    actOnEvent: ConnectResponseBehaviour
  ): Future[String] =
    if (host.isDefined) connectionManager.testAndConnect(host.get) transform {
      case Return(unit) => onConnect(connectReq, actOnEvent)
      case Throw(exc) => connect(connectReq, None, actOnEvent)
    }
    else {
      if (connectionManager.hasAvailableServiceFactory) {
        connectionManager.connection.get.newService() before
          onConnect(connectReq, actOnEvent)
      }
      else connectionManager.findAndConnect() before
        onConnect(connectReq, actOnEvent)
    }

  /**
   * Should perform an action after connecting to a server.
   *
   * @param connectReq a connect request to send
   * @param actOnEvent a decision to make after receiving to connect response
   * @return the currently connected server IP or Exception
   */
  private[this] def onConnect(
    connectReq: => ConnectRequest,
    actOnEvent: ConnectResponseBehaviour
  ): Future[String] = {
    sessionManager.session.currentState.set(States.CONNECTING)
    configureDispatcher() before
      connectionManager.connection.get.serve(
        ReqPacket(None, Some(connectReq))
      ) transform {
      case Return(connectResponse) =>
        val finRep = connectResponse.response.get.asInstanceOf[ConnectResponse]
        actOnEvent(finRep)

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
   * @return the currently connected server IP
   */
  def changeHost(host: Option[String] = None): Future[String] =
    if (host.isDefined)
      connectionManager.hostProvider.testOrFind(host.get) flatMap { server =>
        connectionManager.close() before reconnectWithSession(Some(server))
      }
    else {
      connectionManager.hostProvider.seenRwServer match {
        case Some(rwServer) =>
          if (rwServer == connectionManager.currentHost.get)
            connectionManager.hostProvider.seenRwServer = None
        case None =>
      }

      val newList = connectionManager.hostProvider.serverList
        .filterNot(connectionManager.currentHost.getOrElse("") == _)

      connectionManager.hostProvider.findServer(Some(newList)) flatMap { srv =>
        connectionManager.close() before reconnectWithSession(Some(srv))
      } rescue { case exc =>
        Future(connectionManager.currentHost.getOrElse(""))
      }
    }

  /**
   * Should add some new servers to the host list
   *
   * @param hostList hosts to add
   */
  def addHosts(hostList: String): Unit =
    connectionManager.hostProvider.addHost(hostList)

  /**
   * Should remove some hosts from the host list
   *
   * @param hostList hosts to remove
   * @return Future.Done or Exception
   */
  def removeHosts(hostList: String): Future[Unit] = this.synchronized {
    val hostsToRemove = HostUtilities.formatAndTest(hostList)
    val newList = connectionManager.hostProvider.serverList filterNot {
      hostsToRemove.contains(_)
    }

    connectionManager.currentHost match {
      case Some(currentHost) =>
        if (hostsToRemove.contains(currentHost)) {
          connectionManager.findAndConnect(Some(newList)) flatMap { _ =>
            connectionManager.hostProvider.serverList_=(newList)
            Future.Done
          } rescue { case exc => Future.Done }
        }
        else {
          connectionManager.hostProvider.serverList_=(newList)
          Future.Done
        }
      case None =>
        connectionManager.hostProvider.serverList_=(newList)
        Future.Done
    }
  }

  /**
   * Should find a suitable server and connect to.
   * This operation will interrupt request sending until a new
   * connection and a new session are established
   *
   * @return Future.Done or Exception
   */
  private[finagle] def newSession(host: Option[String]): Future[Unit] =
    if (sessionManager.canCreateSession) {
      val connectReq = sessionManager.buildConnectRequest(sessionTimeout)
      stopJob() before connect(connectReq, host, actOnConnect).unit
    }
    else Future.exception(new SessionAlreadyEstablished(
      "client is already connected to server")) ensure stopJob()


  /**
   * Should reconnect to a suitable server and try to regain the session,
   * or else create a new session.
   *
   * @return the currently connected server
   */
  private[this] def reconnect(
    host: Option[String] = None,
    tries: Int = 0,
    requestBuilder: => ConnectRequest,
    actOnEvent: (Option[String], Int) => ConnectResponseBehaviour
  ): Future[String] =
    if (sessionManager.canReconnect && tries < maxConsecutiveRetries) {
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
   * @return the currently connected server
   */
  private[finagle] def reconnectWithSession(
    host: Option[String] = None,
    tries: Int = 0
  ): Future[String] =
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
   * @return the currently connected server
   */
  private[finagle] def reconnectWithoutSession(
    host: Option[String] = None,
    tries: Int = 0
  ): Future[String] =
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
    connectionManager.hostProvider.startPreventiveSearch()
    actIfRO() before
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