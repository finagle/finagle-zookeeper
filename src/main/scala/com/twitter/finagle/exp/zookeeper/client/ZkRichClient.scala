package com.twitter.finagle.exp.zookeeper.client

import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.OpCode
import com.twitter.finagle.exp.zookeeper._
import com.twitter.finagle.exp.zookeeper.client.managers.{PreProcessService, ClientManager}
import com.twitter.finagle.exp.zookeeper.connection.ConnectionManager
import com.twitter.finagle.exp.zookeeper.data.{ACL, Auth}
import com.twitter.finagle.exp.zookeeper.session.{Session, SessionManager}
import com.twitter.finagle.exp.zookeeper.utils.PathUtils._
import com.twitter.finagle.exp.zookeeper.watch.{Watch, WatchManager}
import com.twitter.logging.Logger
import com.twitter.util.TimeConversions._
import com.twitter.util._

class ZkClient(
  protected[this] val autoReconnect: Boolean = true,
  protected[this] val autoWatchReset: Boolean = true,
  protected[this] val chroot: String = "",
  protected[this] val sessionTimeout: Duration = 3000.milliseconds,
  protected[this] val maxConsecutiveRetries: Option[Int] = Some(10),
  protected[this] val maxReconnectAttempts: Option[Int] = Some(5),
  protected[this] val timeBetweenAttempts: Option[Duration] = Some(30.seconds),
  protected[this] val timeBetweenLinkCheck: Option[Duration] = Some(1000.milliseconds / 2),
  protected[this] val timeBetweenRwSrch: Option[Duration] = Some(1.minute),
  protected[this] val timeBetweenPrevSrch: Option[Duration] = Some(10.minutes),
  hostList: String,
  protected[this] val canReadOnly: Boolean = false
  ) extends Closable with ClientManager {

  private[finagle] val connectionManager = new ConnectionManager(
    hostList,
    canReadOnly,
    timeBetweenPrevSrch,
    timeBetweenRwSrch)
  private[finagle] val sessionManager = new SessionManager(canReadOnly)
  private[finagle] val watchManager: WatchManager = new WatchManager(chroot, autoWatchReset)
  private[finagle] val zkRequestService =
    new PreProcessService(connectionManager, sessionManager, this)
  @volatile protected[this] var authInfo: Set[Auth] = Set()

  def session: Future[Session] = sessionManager.session

  /**
   * To set back auth right after reconnection
   * @return Future.Done if request worked, or exception
   */
  private[finagle] def recoverAuth(): Future[Unit] = {
    val fetches = authInfo.toSeq map { auth =>
      val req = ReqPacket(
        Some(RequestHeader(-4, OpCode.AUTH)),
        Some(new AuthRequest(0, auth))
      )
      connectionManager.connection flatMap { connectn =>
        connectn.serve(req) flatMap { rep =>
          if (rep.err.get == 0) Future.Unit
          else Future.exception(
            ZookeeperException.create("Error while addAuth", rep.err.get))
        }
      }
    }
    Future.join(fetches)
  }

  def addAuth(auth: Auth): Future[Unit] = {
    val req = new AuthRequest(0, auth)

    zkRequestService(req) flatMap { rep =>
      if (rep.err.get == 0) {
        authInfo += auth
        Future.Unit
      } else Future.exception(
        ZookeeperException.create("Error while addAuth", rep.err.get))
    }
  }

  def connect(host: Option[String] = None): Future[Unit] = newSession(host)
  def close(deadline: Time): Future[Unit] = stopJob() before connectionManager.close(deadline)
  def closeService(): Future[Unit] = stopJob() before connectionManager.close()
  def closeSession(): Future[Unit] = disconnect()

  /**
   * We use this to configure the dispatcher, gives connectionManager, WatchManager
   * and SessionManager
   * @return
   */
  private[finagle] def configureDispatcher(): Future[Unit] = {
    val req = ReqPacket(None, Some(ConfigureRequest(
      connectionManager,
      sessionManager,
      watchManager
    )))
    connectionManager.connection flatMap { connectn =>
      connectn.serve(req).unit
    }
  }

  def create(
    path: String,
    data: Array[Byte],
    acl: Array[ACL],
    createMode: Int): Future[String] = {
    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath, createMode)
    ACL.check(acl)
    val req = CreateRequest(finalPath, data, acl, createMode)

    zkRequestService(req) flatMap { rep =>
      if (rep.err.get == 0) {
        val finalRep = rep.response.get.asInstanceOf[CreateResponse]
        Future(finalRep.path.substring(chroot.length))
      } else Future.exception(
        ZookeeperException.create("Error while create", rep.err.get))
    }
  }

  def delete(path: String, version: Int): Future[Unit] = {
    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath)
    val req = DeleteRequest(finalPath, version)

    zkRequestService(req) flatMap { rep =>
      if (rep.err.get == 0) Future.Unit
      else Future.exception(
        ZookeeperException.create("Error while delete", rep.err.get))
    }
  }

  def exists(path: String, watch: Boolean = false): Future[ExistsResponse] = {
    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath)
    val req = ExistsRequest(finalPath, watch)

    zkRequestService(req) flatMap { rep =>
      rep.response match {
        case Some(response: ExistsResponse) =>
          if (watch) {
            val watch = watchManager.register(path, Watch.Type.exists)
            val finalRep = ExistsResponse(response.stat, Some(watch))
            Future(finalRep)
          } else Future(response)

        case None =>
          if (rep.err.get == -101 && watch) {
            val watch = watchManager.register(path, Watch.Type.exists)
            Future(ExistsResponse(None, Some(watch)))
          } else Future.exception(
            ZookeeperException.create("Error while exists", rep.err.get))

        case _ =>
          Future.exception(ZookeeperException.create("Match error while exists"))
      }
    }
  }

  def getACL(path: String): Future[GetACLResponse] = {
    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath)
    val req = GetACLRequest(finalPath)

    zkRequestService(req) flatMap { rep =>
      if (rep.err.get == 0)
        Future(rep.response.get.asInstanceOf[GetACLResponse])
      else Future.exception(
        ZookeeperException.create("Error while getACL", rep.err.get))
    }
  }

  def getChildren(path: String, watch: Boolean = false): Future[GetChildrenResponse] = {
    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath)
    val req = GetChildrenRequest(finalPath, watch)

    zkRequestService(req) flatMap { rep =>
      if (rep.err.get == 0) {
        val res = rep.response.get.asInstanceOf[GetChildrenResponse]
        if (watch) {
          val watch = watchManager.register(path, Watch.Type.exists)
          val childrenList = res.children map (_.substring(chroot.length))
          val finalRep = GetChildrenResponse(childrenList, Some(watch))
          Future(finalRep)
        } else {
          val childrenList = res.children map (_.substring(chroot.length))
          val finalRep = GetChildrenResponse(childrenList, None)
          Future(finalRep)
        }
      } else Future.exception(
        ZookeeperException.create("Error while getChildren", rep.err.get))
    }
  }

  def getChildren2(path: String, watch: Boolean = false): Future[GetChildren2Response] = {
    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath)
    val req = GetChildren2Request(finalPath, watch)

    zkRequestService(req) flatMap { rep =>
      if (rep.err.get == 0) {
        val res = rep.response.get.asInstanceOf[GetChildren2Response]
        if (watch) {
          val watch = watchManager.register(path, Watch.Type.exists)
          val childrenList = res.children map (_.substring(chroot.length))
          val finalRep = GetChildren2Response(childrenList, res.stat, Some(watch))
          Future(finalRep)
        } else {
          val childrenList = res.children map (_.substring(chroot.length))
          val finalRep = GetChildren2Response(childrenList, res.stat, None)
          Future(finalRep)
        }
      } else Future.exception(
        ZookeeperException.create("Error while getChildren2", rep.err.get))
    }
  }

  def getData(path: String, watch: Boolean = false): Future[GetDataResponse] = {
    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath)
    val req = GetDataRequest(finalPath, watch)

    zkRequestService(req) flatMap { rep =>
      if (rep.err.get == 0) {
        val res = rep.response.get.asInstanceOf[GetDataResponse]
        if (watch) {
          val watch = watchManager.register(path, Watch.Type.exists)
          val finalRep = GetDataResponse(res.data, res.stat, Some(watch))
          Future(finalRep)
        } else Future(res)
      } else Future.exception(
        ZookeeperException.create("Error while getData", rep.err.get))
    }
  }

  protected[this] def ping(): Future[Unit] = {
    val req = ReqPacket(Some(RequestHeader(-2, OpCode.PING)), None)

    connectionManager.connection flatMap { connectn =>
      connectn.serve(req) flatMap { rep =>
        if (rep.err.get == 0) Future.Unit
        else Future.exception(
          ZookeeperException.create("Error while ping", rep.err.get))
      }
    }
  }

  def setACL(path: String, acl: Array[ACL], version: Int): Future[SetACLResponse] = {
    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath)
    ACL.check(acl)
    val req = SetACLRequest(finalPath, acl, version)

    zkRequestService(req) flatMap { rep =>
      if (rep.err.get == 0) {
        val res = rep.response.get.asInstanceOf[SetACLResponse]
        Future(res)
      } else Future.exception(
        ZookeeperException.create("Error while setACL", rep.err.get))
    }
  }

  def setData(path: String, data: Array[Byte], version: Int): Future[SetDataResponse] = {
    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath)
    val req = SetDataRequest(finalPath, data, version)

    zkRequestService(req) flatMap { rep =>
      if (rep.err.get == 0) {
        val res = rep.response.get.asInstanceOf[SetDataResponse]
        Future(res)
      } else Future.exception(
        ZookeeperException.create("Error while setData", rep.err.get))
    }
  }

  /**
   * To set watches back right after reconnection
   * @return Future.Done if request worked, or exception
   */
  private[finagle] def setWatches(): Future[Unit] = {
    if (autoWatchReset) {
      sessionManager.session flatMap { sess =>
        val relativeZxid: Long = sess.lastZxid.get
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
          val req = ReqPacket(
            Some(RequestHeader(-8, OpCode.SET_WATCHES)),
            Some(SetWatchesRequest(relativeZxid, dataWatches, existsWatches, childWatches))
          )

          connectionManager.connection flatMap { connectn =>
            connectn.serve(req) flatMap { rep =>
              if (rep.err.get == 0) Future.Done
              else {
                Future.exception(
                  ZookeeperException.create("Error while setWatches", rep.err.get))
                Future.Done
              }
            }
          }
        } else
          Future.Done
      }
    }
    else {
      watchManager.clearWatches()
      Future.Done
    }
  }

  def sync(path: String): Future[SyncResponse] = {
    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath)
    val req = SyncRequest(finalPath)

    zkRequestService(req) flatMap {
      rep =>
        if (rep.err.get == 0) {
          val res = rep.response.get.asInstanceOf[SyncResponse]
          val finalRep = SyncResponse(res.path.substring(chroot.length))
          Future(finalRep)
        } else Future.exception(
          ZookeeperException.create("Error while sync", rep.err.get))
    }
  }

  def transaction(opList: Seq[OpRequest]): Future[TransactionResponse] = {
    Transaction.prepareAndCheck(opList, chroot) match {
      case Return(res) =>
        val req = new TransactionRequest(res)

        zkRequestService(req) flatMap {
          rep =>
            if (rep.err.get == 0) {
              val res = rep.response.get.asInstanceOf[TransactionResponse]
              val finalOpList = Transaction.formatPath(res.responseList, chroot)
              Future(TransactionResponse(finalOpList))
            } else Future.exception(
              ZookeeperException.create("Error while transaction", rep.err.get))
        }
      case Throw(exc) => Future.exception(exc)
    }
  }
}

object ZkClient {
  private[finagle] val logger = Logger("Finagle-zookeeper")
}