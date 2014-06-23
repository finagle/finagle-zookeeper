package com.twitter.finagle.exp.zookeeper.client

import com.twitter.finagle.CancelledRequestException
import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.OpCode
import com.twitter.finagle.exp.zookeeper._
import com.twitter.finagle.exp.zookeeper.connection.Connection
import com.twitter.finagle.exp.zookeeper.session.Session.States
import com.twitter.finagle.exp.zookeeper.utils.PathUtils._
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.{Throw, Return, Future}
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Autolive == Automatic Session and Connection management (buzzworld ok...)
 */
trait Autolive {self: ZkClient =>

  private[this] val isCheckingLink = new AtomicBoolean(false)
  private[this] val canRunStateLoop = new AtomicBoolean(false)

  protected[this] def prepareClose(): Future[Unit] = {
    sessionManager.session.canClose()
    sessionManager.session.prepareClose()
    canRunStateLoop.set(false)
    connectionManager.PreventiveSearchScheduler.currentTask.get.cancel()
    Future.Unit
  }

  private[this] def checkLink(tries: Int = 0): Future[Unit] = checkSession() rescue {
    case exc: ZookeeperException =>
      if (tries < maxReconnectAttempts)
        checkLink(tries + 1).delayed(timeBetweenAttempts)(DefaultTimer.twitter)
      else Future.exception(exc)
    case exc => Future.exception(exc)
  } before checkConnection() rescue {
    case exc: ZookeeperException =>
      if (tries < maxReconnectAttempts)
        checkLink(tries + 1).delayed(timeBetweenAttempts)(DefaultTimer.twitter)
      else Future.exception(exc)
    case exc => Future.exception(exc)
  }

  private[this] def checkConnection(): Future[Unit] = {
    if (!connectionManager.connection.get.isValid.get() &&
      !sessionManager.session.isClosingSession.get()) {
      canRunStateLoop.set(false)
      reconnectWithSession()
    } else {
      Future.Unit
    }
  }

  /**
   * This method is called each time we try to write on the Transport
   * to make sure the connection is still alive. If it's not then it can
   * try to reconnect ( if the sessionManager.session has not expired ) or create a new sessionManager.session
   * if the sessionManager.session has expired. It won't connect if the client has never connected
   */
  protected[this] def checkSession(): Future[Unit] = {
    if (!sessionManager.session.isFirstConnect.get()) {
      if (sessionManager.session.state == States.CONNECTION_LOSS) {
        sessionManager.session.pingScheduler.currentTask.get.cancel()
        canRunStateLoop.set(false)
        // We can try to reconnect with last zxid and set the watches back
        // try reconnect with the same connection and session
        reconnectWithSession()

      } else if (sessionManager.session.state == States.SESSION_MOVED) {
        // The sessionManager.session has moved to another server
        // TODO CHECK : I think we should stop connecting if SESSION_MOVED
        sessionManager.session.pingScheduler.currentTask.get.cancel()
        canRunStateLoop.set(false)
        sessionManager.session.prepareClose()
        sessionManager.session.close()
        // todo close service factory
        Future.exception(SessionMovedException("Session has moved to another server"))

      } else if (sessionManager.session.state == States.SESSION_EXPIRED) {
        sessionManager.session.pingScheduler.currentTask.get.cancel()
        canRunStateLoop.set(false)
        // Reconnect with a new sessionManager.session and same connection
        reconnect()

      } else if (sessionManager.session.isReadOnly) {
        if (sessionManager.isSearchingRwServer) Future.Unit
        else {
          connectionManager.searchRwServer(timeBetweenRwSvrSrch) flatMap {
            factory =>
              connectionManager.connection = Some(new Connection(factory))
              reconnectWithSession()
          }
        }
      } else {
        // Everything is doing good
        Future.Unit
      }
    } else {
      // We should be CONNECTING while firstConnect
      if (sessionManager.session.state != States.CONNECTING) // todo
        throw new RuntimeException("No connection established exception: " +
          "Did you ever connected to the server ? " + sessionManager.session.state)
      else Future.Unit
    }
  }

  /**
   * This will try to reconnect using Session id and password
   * if this does not succeed, it will reconnect with a new Session
   * @return
   */
  private[this] def reconnectWithSession(triesCounter: Int = 0): Future[Unit] = {
    val connectReq = ConnectRequest(
      0,
      sessionManager.session.lastZxid,
      sessionManager.session.sessionTimeout,
      if (!sessionManager.hasConnectedRwServer) 0L else sessionManager.session.sessionId,
      sessionManager.session.sessionPassword,
      readOnly
    )

    if (triesCounter <= maxConsecutiveRetries)
      if (connectionManager.connection.get.isValid.get()) {
        sessionManager.session.canConnect()
        sessionManager.session.state = States.CONNECTING
        // we can try to reconnect with the same connection
        preConfigureDispatcher() before
          connectionManager.connection.get
            .serve(ReqPacket(None, Some(connectReq))) transform {
          case Return(connectResponse) =>
            sessionManager.session.parseStateHeader(connectResponse.header)
            val finalRep = connectResponse.response.get.asInstanceOf[ConnectResponse]
            if (!finalRep.isRO) {
              sessionManager.session.isReadOnly = false
              sessionManager.hasConnectedRwServer = true
            } else {
              sessionManager.session.isReadOnly = true
            }
            sessionManager.session.startPing(ping())

            // Loop checking connection and session state
            runLinkChecker()

            configureDispatcher()

          case Throw(exc: CancelledRequestException) =>
            // problem with the current connection
            // find a new RW server and reconnectWithSession
            sessionManager.session.state = States.CLOSED
            connectionManager.findNextServer(connectionManager.hostList) flatMap { server =>
              connectionManager.connection = Some(new Connection(server))
              reconnectWithSession(triesCounter + 1)
            }

          case Throw(exc: Throwable) =>
            // problem with the session
            // connect to the server with a new session
            sessionManager.session.state = States.CLOSED
            reconnect()
        }
      } else {
        // we need to find a new server to connect to
        // find next RW server
        // connect with current ids
        connectionManager.findNextServer(connectionManager.hostList) flatMap { server =>
          connectionManager.connection = Some(new Connection(server))
          reconnectWithSession(triesCounter + 1)
        }
      }
    else {
      Future.exception(ZookeeperException.create("could not reconnect to server"))
    }
  }

  /**
   * Reconnect to a server with a new Session
   * @return when the client will be connected again
   */
  private[this] def reconnect(triesCounter: Int = 0): Future[Unit] = {
    // reconnect with a new Session
    val connectReq = ConnectRequest(
      sessionTimeout = sessionManager.session.sessionTimeout,
      canBeRO = readOnly
    )

    if (triesCounter <= maxConsecutiveRetries)
      if (connectionManager.connection.get.isValid.get()) {
        sessionManager.session.canConnect()
        sessionManager.session.state = States.CONNECTING
        // we can try to reconnect with the same connection
        preConfigureDispatcher() before
          connectionManager.connection.get
            .serve(ReqPacket(None, Some(connectReq))) transform {
          case Return(connectResponse) =>
            sessionManager.session.parseStateHeader(connectResponse.header)
            val finalRep = connectResponse.response.get.asInstanceOf[ConnectResponse]
            if (!finalRep.isRO) {
              sessionManager.session.isReadOnly = false
              sessionManager.hasConnectedRwServer = true
            } else {
              sessionManager.session.isReadOnly = true
            }
            sessionManager.initSession(finalRep, sessionManager.session.sessionTimeout)
            sessionManager.session.startPing(ping())

            // Loop checking connection and session state
            runLinkChecker()

            configureDispatcher()

          case Throw(exc: Throwable) =>
            sessionManager.session.state = States.CLOSED
            // will try with a new connection
            connectionManager.findNextServer(connectionManager.hostList) flatMap { server =>
              connectionManager.connection = Some(new Connection(server))
              reconnect(triesCounter + 1)
            }
        }
      } else {
        // we need to find a new server to connect to
        // find next RW server
        // connect with current ids
        connectionManager.findNextServer(connectionManager.hostList) flatMap { server =>
          connectionManager.connection = Some(new Connection(server))
          reconnect(triesCounter + 1)
        }
      } else {
      Future.exception(ZookeeperException.create("could not reconnect to server"))
    }
  }

  protected[this] def runLinkChecker(): Future[Unit] = {
    canRunStateLoop.set(true)
    if (!isCheckingLink.getAndSet(true)) stateLoop()
    else Future.Unit
  }

  // Only use this on reconnectionWithSession
  private[this] def setWatches(): Future[Unit] = {
    if (autoWatchReset) {
      val relativeZxid: Long = sessionManager.session.lastZxid
      val dataWatches: Seq[String] = watchManager.getDataWatches.keySet.map { path =>
        prependChroot(path, chroot)
      }.toSeq
      val existsWatches: Seq[String] = watchManager.getExistsWatches.keySet.map { path =>
        prependChroot(path, chroot)
      }.toSeq
      val childWatches: Seq[String] = watchManager.getChildWatches.keySet.map { path =>
        prependChroot(path, chroot)
      }.toSeq

      if (dataWatches.nonEmpty || existsWatches.nonEmpty || childWatches.nonEmpty) {
        checkSession()
        val req = ReqPacket(
          Some(RequestHeader(-8, OpCode.SET_WATCHES)),
          Some(SetWatchesRequest(relativeZxid, dataWatches, existsWatches, childWatches)))

        connectionManager.connection.get.serve(req) flatMap { rep =>
          sessionManager.session.parseStateHeader(rep.header)

          if (rep.header.err == 0) {
            Future.Unit
          } else {
            Future.exception(ZookeeperException.create("Error while setWatches", rep.header.err))
          }
        }
      } else {
        Future.Unit
      }

    } else Future.Unit
  }

  private[this] def addAuth(): Future[Unit] = {
    val fetches = authInfo.toSeq map { auth =>
      addAuth(auth)
    }
    Future.join(fetches)
  }

  protected[this] def stateLoop(): Future[Unit] = {
    if (!sessionManager.session.isClosingSession.get() && canRunStateLoop.get())
      checkLink().delayed(connectionTimeout)(DefaultTimer.twitter) before stateLoop()
    else {
      isCheckingLink.set(false)
      Future.Done
    }
  }
}