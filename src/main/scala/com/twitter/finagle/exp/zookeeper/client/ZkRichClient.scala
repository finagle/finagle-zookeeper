package com.twitter.finagle.exp.zookeeper.client

import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.{CancelledRequestException, IndividualRequestTimeoutException}
import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.OpCode
import com.twitter.finagle.exp.zookeeper._
import com.twitter.finagle.exp.zookeeper.connection.ConnectionManager
import com.twitter.finagle.exp.zookeeper.data.{ACL, Auth}
import com.twitter.finagle.exp.zookeeper.session.Session.States
import com.twitter.finagle.exp.zookeeper.session.SessionManager
import com.twitter.finagle.exp.zookeeper.utils.PathUtils._
import com.twitter.finagle.exp.zookeeper.watch.{WatchManager, WatchType}
import com.twitter.logging.Logger
import com.twitter.util.TimeConversions._
import com.twitter.util._

class ZkClient(
  protected[this] val autoReconnect: Boolean = true,
  protected[this] val autoWatchReset: Boolean = true,
  chRoot: Option[String] = None,
  protected[this] val connectionTimeout: Duration = 1000.milliseconds,
  protected[this] val maxConsecutiveRetries: Int = 10,
  protected[this] val maxReconnectAttempts: Int = 5,
  protected[this] val timeBetweenAttempts: Duration = 30.seconds,
  protected[this] val timeBetweenRwSvrSrch: Duration = 1.minute,
  protected[this] val timeBetweenPrevntvSrch: Option[Duration] = Some(10.minutes),
  hostList: String,
  protected[this] val readOnly: Boolean = false
  ) extends Closable with Autolive {

  protected[this] val chroot = chRoot.getOrElse("")
  // fixme server lookup should happen during connect request
  protected[this] val connectionManager =
    new ConnectionManager(timeBetweenPrevntvSrch, hostList)
  protected[this] val sessionManager =
    new SessionManager(autoReconnect, autoWatchReset, this)
  protected[this] val watchManager: WatchManager = new WatchManager(chroot)
  @volatile protected[this] var authInfo: Set[Auth] = Set()

  def getSessionId: Long = sessionManager.session.sessionId
  def getSessionPwd: Array[Byte] = sessionManager.session.sessionPassword
  def getTimeout: Duration = sessionManager.session.sessionTimeout

  def addAuth(auth: Auth): Future[Unit] = {
    checkSession()
    // TODO check auth ?
    val req = ReqPacket(
      Some(RequestHeader(-4, OpCode.AUTH)),
      Some(new AuthRequest(0, auth)))
    // todo readOnly check ?
    connectionManager.connection.get.serve(req)
      .raiseWithin(DefaultTimer.twitter, sessionManager.session.readTimeout,
        new IndividualRequestTimeoutException(sessionManager.session.readTimeout))
      .flatMap { rep =>
      sessionManager.session.parseStateHeader(rep.header)
      authInfo += auth
      if (rep.header.err == 0) {
        Future.Unit
      } else {
        Future.exception(ZookeeperException.create("Error while addAuth", rep.header.err))
      }
    } rescue {
      case exc: IndividualRequestTimeoutException =>
        sessionManager.session.state = States.CONNECTION_LOSS
        checkSession() before Future.exception(new CancelledRequestException(exc))
    }
  }

  def connect(timeOut: Duration = 2000.milliseconds): Future[ConnectResponse] = {
    sessionManager.session.canConnect()
    sessionManager.session.state = States.CONNECTING

    val req = ReqPacket(
      None,
      Some(new ConnectRequest(0, 0L, timeOut, canBeRO = readOnly)))

    connectionManager.initConnection() before preConfigureDispatcher() before
      connectionManager.connection.get.serve(req)
        .raiseWithin(DefaultTimer.twitter, sessionManager.session.readTimeout,
          new IndividualRequestTimeoutException(sessionManager.session.readTimeout))
        .flatMap { rep =>
        sessionManager.session.parseStateHeader(rep.header)
        val finalRep = rep.response.get.asInstanceOf[ConnectResponse]
        if (!finalRep.isRO) {
          sessionManager.session.isReadOnly = false
          sessionManager.hasConnectedRwServer = true
        } else {
          sessionManager.session.isReadOnly = true
        }
        sessionManager.initSession(finalRep, timeOut)
        sessionManager.session.startPing(ping())

        // Loop checking connection and session state
        runLinkChecker()

        /*println("---> CONNECT | timeout: " +
          finalRep.timeOut + " | sessionManager.sessionID: " +
          finalRep.sessionManager.sessionId + " | canRO: " +
          finalRep.canRO.getOrElse(false))*/

        configureDispatcher() before Future(finalRep)
      } onFailure { exc =>
      sessionManager.session.state = States.CLOSED
      Future.exception(exc)
    } rescue {
      case exc: IndividualRequestTimeoutException =>
        sessionManager.session.state = States.CONNECTION_LOSS
        checkSession() before Future.exception(new CancelledRequestException(exc))
    }
  }

  def close(deadline: Time): Future[Unit] = connectionManager.connection.get.close(deadline)
  def closeService(): Future[Unit] = connectionManager.connection.get.close()
  def closeSession(): Future[Unit] = {

    checkSession() before prepareClose()

    val req = ReqPacket(Some(RequestHeader(1, OpCode.CLOSE_SESSION)), None)

    connectionManager.connection.get.serve(req)
      .raiseWithin(DefaultTimer.twitter, sessionManager.session.readTimeout,
        new IndividualRequestTimeoutException(sessionManager.session.readTimeout))
      .flatMap { rep =>
      sessionManager.session.parseStateHeader(rep.header)
      if (rep.header.err == 0) {
        Future.Unit
      } else {
        Future.exception(ZookeeperException.create("Error while close", rep.header.err))
      }
    } onSuccess (_ => sessionManager.session.close()) rescue {
      case exc: IndividualRequestTimeoutException =>
        sessionManager.session.state = States.CONNECTION_LOSS
        checkSession() before Future.exception(new CancelledRequestException(exc))
    }
  }

  /**
   * We use this to preconfigure the dispatcher, gives connectionManager, WatchManager
   * and SessionManager
   * @return
   */
  protected[this] def preConfigureDispatcher(): Future[Unit] = {
    val req = ReqPacket(None, Some(ConfigureRequest(
      Some(connectionManager),
      Some(sessionManager),
      Some(watchManager)
    )))
    connectionManager.connection.get.serve(req).unit
  }

  /**
   * We use this to finish dispatcher configuration, it says the dispatcher that
   * it can initiate a new session
   * @return
   */
  protected[this] def configureDispatcher(): Future[Unit] = {
    val req = ReqPacket(None, Some(ConfigureRequest(
      None,
      None,
      None
    )))
    connectionManager.connection.get.serve(req).unit
  }

  def create(
    path: String,
    data: Array[Byte],
    acl: Array[ACL],
    createMode: Int): Future[String] = {
    // todo readOnly check
    checkSession()
    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath, createMode)
    ACL.check(acl)
    val req = ReqPacket(
      Some(RequestHeader(sessionManager.session.getXid, OpCode.CREATE)),
      Some(CreateRequest(finalPath, data, acl, createMode)))

    connectionManager.connection.get.serve(req)
      .raiseWithin(DefaultTimer.twitter, sessionManager.session.readTimeout,
        new IndividualRequestTimeoutException(sessionManager.session.readTimeout))
      .flatMap { rep =>
      sessionManager.session.parseStateHeader(rep.header)

      if (rep.header.err == 0) {
        sessionManager.session.parseStateHeader(rep.header)
        val finalRep = rep.response.get.asInstanceOf[CreateResponse]
        Future(finalRep.path.substring(chroot.length))
      } else {
        Future.exception(ZookeeperException.create("Error while create", rep.header.err))
      }
    } rescue {
      case exc: IndividualRequestTimeoutException =>
        sessionManager.session.state = States.CONNECTION_LOSS
        checkSession() before Future.exception(new CancelledRequestException(exc))
    }
  }

  def delete(path: String, version: Int): Future[Unit] = {
    // todo readOnly check
    checkSession()
    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath)
    val req = ReqPacket(
      Some(RequestHeader(sessionManager.session.getXid, OpCode.DELETE)),
      Some(DeleteRequest(finalPath, version))
    )

    connectionManager.connection.get.serve(req)
      .raiseWithin(DefaultTimer.twitter, sessionManager.session.readTimeout,
        new IndividualRequestTimeoutException(sessionManager.session.readTimeout))
      .flatMap { rep =>
      sessionManager.session.parseStateHeader(rep.header)

      if (rep.header.err == 0) {
        sessionManager.session.parseStateHeader(rep.header)
        Future.Unit
      } else {
        Future.exception(ZookeeperException.create("Error while getACL", rep.header.err))
      }
    } rescue {
      case exc: IndividualRequestTimeoutException =>
        sessionManager.session.state = States.CONNECTION_LOSS
        checkSession() before Future.exception(new CancelledRequestException(exc))
    }
  }

  def exists(path: String, watch: Boolean = false): Future[ExistsResponse] = {

    checkSession()
    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath)
    val req = ReqPacket(
      Some(RequestHeader(sessionManager.session.getXid, OpCode.EXISTS)),
      Some(ExistsRequest(finalPath, watch)))

    connectionManager.connection.get.serve(req)
      .raiseWithin(DefaultTimer.twitter, sessionManager.session.readTimeout,
        new IndividualRequestTimeoutException(sessionManager.session.readTimeout))
      .flatMap { rep =>
      sessionManager.session.parseStateHeader(rep.header)

      rep.response match {
        case Some(response: NodeWithWatch) =>
          if (watch) {
            val watch = watchManager.register(path, WatchType.exists)
            val finalRep = NodeWithWatch(response.stat, Some(watch))
            Future(finalRep)
          } else {
            Future(response)
          }
        case None =>
          if (rep.header.err == -101 && watch) {
            val watch = watchManager.register(path, WatchType.exists)
            Future(NoNodeWatch(watch))
          } else {
            Future.exception(ZookeeperException.create("Error while exists", rep.header.err))
          }
        case _ =>
          Future.exception(ZookeeperException.create("Match exception while exists"))
      }
    } rescue {
      case exc: IndividualRequestTimeoutException =>
        sessionManager.session.state = States.CONNECTION_LOSS
        checkSession() before Future.exception(new CancelledRequestException(exc))
    }
  }


  def getACL(path: String): Future[GetACLResponse] = {

    checkSession()
    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath)
    val req = ReqPacket(
      Some(RequestHeader(sessionManager.session.getXid, OpCode.GET_ACL)),
      Some(GetACLRequest(finalPath)))

    connectionManager.connection.get.serve(req)
      .raiseWithin(DefaultTimer.twitter, sessionManager.session.readTimeout,
        new IndividualRequestTimeoutException(sessionManager.session.readTimeout))
      .flatMap { rep =>
      sessionManager.session.parseStateHeader(rep.header)

      if (rep.header.err == 0) {
        Future(rep.response.get.asInstanceOf[GetACLResponse])
      } else {
        Future.exception(ZookeeperException.create("Error while getACL", rep.header.err))
      }
    } rescue {
      case exc: IndividualRequestTimeoutException =>
        sessionManager.session.state = States.CONNECTION_LOSS
        checkSession() before Future.exception(new CancelledRequestException(exc))
    }
  }

  def getChildren(path: String, watch: Boolean = false): Future[GetChildrenResponse] = {

    checkSession()
    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath)
    val req = ReqPacket(
      Some(RequestHeader(sessionManager.session.getXid, OpCode.GET_CHILDREN)),
      Some(GetChildrenRequest(finalPath, watch)))

    connectionManager.connection.get.serve(req)
      .raiseWithin(DefaultTimer.twitter, sessionManager.session.readTimeout,
        new IndividualRequestTimeoutException(sessionManager.session.readTimeout))
      .flatMap { rep =>
      sessionManager.session.parseStateHeader(rep.header)

      if (rep.header.err == 0) {
        val res = rep.response.get.asInstanceOf[GetChildrenResponse]
        if (watch) {
          val watch = watchManager.register(path, WatchType.exists)
          val childrenList = res.children map (_.substring(chroot.length))
          val finalRep = GetChildrenResponse(childrenList, Some(watch))
          Future(finalRep)
        } else {
          val childrenList = res.children map (_.substring(chroot.length))
          val finalRep = GetChildrenResponse(childrenList, None)
          Future(finalRep)
        }
      } else {
        Future.exception(ZookeeperException.create("Error while getChildren", rep.header.err))
      }
    } rescue {
      case exc: IndividualRequestTimeoutException =>
        sessionManager.session.state = States.CONNECTION_LOSS
        checkSession() before Future.exception(new CancelledRequestException(exc))
    }
  }

  def getChildren2(path: String, watch: Boolean = false): Future[GetChildren2Response] = {

    checkSession()
    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath)
    val req = ReqPacket(
      Some(RequestHeader(sessionManager.session.getXid, OpCode.GET_CHILDREN2)),
      Some(GetChildren2Request(finalPath, watch)))

    connectionManager.connection.get.serve(req)
      .raiseWithin(DefaultTimer.twitter, sessionManager.session.readTimeout,
        new IndividualRequestTimeoutException(sessionManager.session.readTimeout))
      .flatMap { rep =>
      sessionManager.session.parseStateHeader(rep.header)

      if (rep.header.err == 0) {
        val res = rep.response.get.asInstanceOf[GetChildren2Response]
        if (watch) {
          val watch = watchManager.register(path, WatchType.exists)
          val childrenList = res.children map (_.substring(chroot.length))
          val finalRep = GetChildren2Response(childrenList, res.stat, Some(watch))
          Future(finalRep)
        } else {
          val childrenList = res.children map (_.substring(chroot.length))
          val finalRep = GetChildren2Response(childrenList, res.stat, None)
          Future(finalRep)
        }
      } else {
        Future.exception(ZookeeperException.create("Error while getChildren2", rep.header.err))
      }
    } rescue {
      case exc: IndividualRequestTimeoutException =>
        sessionManager.session.state = States.CONNECTION_LOSS
        checkSession() before Future.exception(new CancelledRequestException(exc))
    }
  }

  def getData(path: String, watch: Boolean = false): Future[GetDataResponse] = {

    checkSession()
    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath)
    val req = ReqPacket(
      Some(RequestHeader(sessionManager.session.getXid, OpCode.GET_DATA)),
      Some(GetDataRequest(finalPath, watch)))

    connectionManager.connection.get.serve(req)
      .raiseWithin(DefaultTimer.twitter, sessionManager.session.readTimeout,
        new IndividualRequestTimeoutException(sessionManager.session.readTimeout))
      .flatMap { rep =>
      sessionManager.session.parseStateHeader(rep.header)

      if (rep.header.err == 0) {
        val res = rep.response.get.asInstanceOf[GetDataResponse]
        if (watch) {
          val watch = watchManager.register(path, WatchType.exists)
          val finalRep = GetDataResponse(res.data, res.stat, Some(watch))
          Future(finalRep)
        } else {
          Future(res)
        }
      } else {
        Future.exception(ZookeeperException.create("Error while getData", rep.header.err))
      }
    } rescue {
      case exc: IndividualRequestTimeoutException =>
        sessionManager.session.state = States.CONNECTION_LOSS
        checkSession() before Future.exception(new CancelledRequestException(exc))
    }
  }

  // GetMaxChildren is implemented but not available in the java lib
  /*def getMaxChildren(path: String, xid: Int): Future[Response] = {
    /*PathUtils.validatePath(path, createMode)
    val finalPath = PathUtils.prependChroot(path, null)*/
    println("<--getMaxChildren: " + xid)

    val header = RequestHeader(xid, ?)
    val body = GetDataRequestBody(path, false) // false because watch's not supported

    connectionManager.serve(new GetDataRequest(header, body))
  }*/

  protected[this] def ping(): Future[Unit] = {

    checkSession()
    val req = ReqPacket(Some(RequestHeader(-2, OpCode.PING)), None)

    connectionManager.connection.get.serve(req)
      .raiseWithin(DefaultTimer.twitter, sessionManager.session.readTimeout,
        new IndividualRequestTimeoutException(sessionManager.session.readTimeout))
      .flatMap { rep =>
      sessionManager.session.parseStateHeader(rep.header)
      if (rep.header.err == 0) {
        Future.Unit
      } else {
        Future.exception(ZookeeperException.create("Error while ping", rep.header.err))
      }
    } rescue {
      case exc: IndividualRequestTimeoutException =>
        sessionManager.session.state = States.CONNECTION_LOSS
        checkSession() before Future.exception(new CancelledRequestException(exc))
    }
  }

  def setACL(path: String, acl: Array[ACL], version: Int): Future[SetACLResponse] = {
    // todo readOnly check
    checkSession()
    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath)
    ACL.check(acl)
    val req = ReqPacket(
      Some(RequestHeader(sessionManager.session.getXid, OpCode.SET_ACL)),
      Some(SetACLRequest(finalPath, acl, version)))

    connectionManager.connection.get.serve(req)
      .raiseWithin(DefaultTimer.twitter, sessionManager.session.readTimeout,
        new IndividualRequestTimeoutException(sessionManager.session.readTimeout))
      .flatMap { rep =>
      sessionManager.session.parseStateHeader(rep.header)

      if (rep.header.err == 0) {
        val res = rep.response.get.asInstanceOf[SetACLResponse]
        Future(res)
      } else {
        Future.exception(ZookeeperException.create("Error while setACL", rep.header.err))
      }
    } rescue {
      case exc: IndividualRequestTimeoutException =>
        sessionManager.session.state = States.CONNECTION_LOSS
        checkSession() before Future.exception(new CancelledRequestException(exc))
    }
  }

  def setData(path: String, data: Array[Byte], version: Int): Future[SetDataResponse] = {
    // todo readOnly check
    checkSession()
    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath)
    val req = ReqPacket(
      Some(RequestHeader(sessionManager.session.getXid, OpCode.SET_DATA)),
      Some(SetDataRequest(finalPath, data, version)))

    connectionManager.connection.get.serve(req)
      .raiseWithin(DefaultTimer.twitter, sessionManager.session.readTimeout,
        new IndividualRequestTimeoutException(sessionManager.session.readTimeout))
      .flatMap { rep =>
      sessionManager.session.parseStateHeader(rep.header)
      if (rep.header.err == 0) {
        val res = rep.response.get.asInstanceOf[SetDataResponse]
        Future(res)
      } else {
        Future.exception(ZookeeperException.create("Error while setData", rep.header.err))
      }
    } rescue {
      case exc: IndividualRequestTimeoutException =>
        sessionManager.session.state = States.CONNECTION_LOSS
        checkSession() before Future.exception(new CancelledRequestException(exc))
    }
  }

  def sync(path: String): Future[SyncResponse] = {

    checkSession()
    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath)
    val req = ReqPacket(
      Some(RequestHeader(sessionManager.session.getXid, OpCode.SYNC)),
      Some(SyncRequest(finalPath)))

    connectionManager.connection.get.serve(req)
      .raiseWithin(DefaultTimer.twitter, sessionManager.session.readTimeout,
        new IndividualRequestTimeoutException(sessionManager.session.readTimeout))
      .flatMap { rep =>
      sessionManager.session.parseStateHeader(rep.header)

      if (rep.header.err == 0) {
        val res = rep.response.get.asInstanceOf[SyncResponse]
        val finalRep = SyncResponse(res.path.substring(chroot.length))
        Future(finalRep)
      } else {
        Future.exception(ZookeeperException.create("Error while sync", rep.header.err))
      }
    } rescue {
      case exc: IndividualRequestTimeoutException =>
        sessionManager.session.state = States.CONNECTION_LOSS
        checkSession() before Future.exception(new CancelledRequestException(exc))
    }
  }

  def transaction(opList: Array[OpRequest]): Future[TransactionResponse] = {
    // todo readOnly check
    checkSession()
    Transaction.prepareAndCheck(opList, chroot) match {
      case Return(res) =>
        val transaction = new Transaction(res)
        val req = ReqPacket(
          Some(RequestHeader(sessionManager.session.getXid, OpCode.MULTI)),
          Some(new TransactionRequest(transaction)))

        connectionManager.connection.get.serve(req)
          .raiseWithin(DefaultTimer.twitter, sessionManager.session.readTimeout,
            new IndividualRequestTimeoutException(sessionManager.session.readTimeout))
          .flatMap { rep =>
          sessionManager.session.parseStateHeader(rep.header)
          // fixme return partial result
          if (rep.header.err == 0) {
            val res = rep.response.get.asInstanceOf[TransactionResponse]
            val finalOpList = Transaction.formatPath(res.responseList, chroot)
            Future(TransactionResponse(finalOpList))
          } else {
            Future.exception(ZookeeperException.create("Error while transaction", rep.header.err))
          }
        } rescue {
          case exc: IndividualRequestTimeoutException =>
            sessionManager.session.state = States.CONNECTION_LOSS
            checkSession() before Future.exception(new CancelledRequestException(exc))
        }
      case Throw(exc) => Future.exception(exc)
    }
  }
}

object ZkClient {
  private[this] val logger = Logger("Finagle-zookeeper")
  def getLogger = logger

  def apply(hostsList: String): ZkClient = {
    new ZkClient(hostList = hostsList)
  }
}