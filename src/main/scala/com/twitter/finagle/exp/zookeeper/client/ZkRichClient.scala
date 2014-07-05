package com.twitter.finagle.exp.zookeeper.client

import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.OpCode
import com.twitter.finagle.exp.zookeeper._
import com.twitter.finagle.exp.zookeeper.client.managers.ClientManager
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

  def session: Session = sessionManager.session

  // todo implement create2, removeWatches, checkWatches, ReconfigRequest, check ?

  /**
   * Add the specified Auth(scheme:data) information to this connection.
   *
   * @param auth an Auth
   * @return Future[Unit] or Exception
   */
  def addAuth(auth: Auth): Future[Unit] = {
    val req = new AuthRequest(0, auth)

    zkRequestService(req) flatMap { rep =>
      if (rep.err.get == 0) {
        authInfo += auth
        Future.Unit
      } else if (rep.err.get == -115) {
        watchManager.process(
          WatchEvent(Watch.EventType.NONE, Watch.EventState.AUTH_FAILED, ""))
        Future.exception(
          ZookeeperException.create("Error while addAuth", rep.err.get))
      }
      else Future.exception(
        ZookeeperException.create("Error while addAuth", rep.err.get))
    }
  }

  /**
   * Connects to a host or finds an available server, then creates a session.
   *
   * @param host a server to connect to
   * @return Future[Unit] or Exception
   */
  def connect(host: Option[String] = None): Future[Unit] = newSession(host)

  /**
   * Stops background jobs and closes the connection. Should be used after
   * closing the session.
   *
   * @param deadline a deadline for closing
   * @return Future[Unit] or Exception
   */
  def close(deadline: Time): Future[Unit] = stopJob() before connectionManager.close(deadline)

  /**
   * Stops background jobs and closes the connection. Should be used after
   * closing the session.
   *
   * @return Future[Unit] or Exception
   */
  def closeService(): Future[Unit] = stopJob() before connectionManager.close()

  /**
   * Closes the session and stops background jobs.
   *
   * @return Future[Unit] or Exception
   */
  def closeSession(): Future[Unit] = disconnect()

  /**
   * We use this to configure the dispatcher, gives connectionManager, WatchManager
   * and SessionManager
   *
   * @return Future.Done
   */
  private[finagle] def configureDispatcher(): Future[Unit] = {
    val req = ReqPacket(None, Some(ConfigureRequest(
      connectionManager,
      sessionManager,
      watchManager
    )))
    connectionManager.connection.get.serve(req).unit
  }

  /**
   * Create a node with the given path. The node data will be the given data,
   * and node acl will be the given acl.
   *
   * @param path the path for the node
   * @param data the initial data for the node
   * @param acl  the acl for the node
   * @param createMode specifying whether the node to be created is ephemeral
   *                   and/or sequential
   * @return Future[String] or Exception
   */
  def create(
    path: String,
    data: Array[Byte],
    acl: Array[ACL],
    createMode: Int) = {
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

  /**
   * Delete the node with the given path. The call will succeed if such a node
   * exists, and the given version matches the node's version (if the given
   * version is -1, it matches any node's versions).
   *
   * @param path the path of the node to be deleted.
   * @param version the expected node version.
   * @return Future[Unit] or Exception
   */
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

  /**
   * Return the stat of the node of the given path. Return null if no such a
   * node exists.
   *
   * If the watch is non-null and the call is successful (no exception is thrown),
   * a watch will be left on the node with the given path. The watch will be
   * triggered by a successful operation that creates/delete the node or sets
   * the data on the node.
   *
   * @param path the node path
   * @param watch a boolean to set a watch or not
   * @return
   */
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

  /**
   * Return the ACL and stat of the node of the given path.
   *
   * @param path the node path
   * @return Future[GetACLResponse] or Exception
   */
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

  /**
   * For the given znode path return the stat and children list.
   *
   * If the watch is true and the call is successful (no exception is thrown),
   * a watch will be left on the node with the given path. The watch will be
   * triggered by a successful operation that deletes the node of the given
   * path or creates/delete a child under the node.
   *
   * @param path the node path
   * @param watch a boolean to set a watch or not
   * @return Future[GetChildrenResponse] or Exception
   */
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

  /**
   * For the given znode path return the stat and children list.
   *
   * If the watch is true and the call is successful (no exception is thrown),
   * a watch will be left on the node with the given path. The watch will be
   * triggered by a successful operation that deletes the node of the given
   * path or creates/delete a child under the node.
   *
   * @param path the node path
   * @param watch a boolean to set a watch or not
   * @return Future[GetChildren2Response] or Exception
   */
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

  /**
   * Return the data and the stat of the node of the given path.
   *
   * If the watch is non-null and the call is successful (no exception is
   * thrown), a watch will be left on the node with the given path. The watch
   * will be triggered by a successful operation that sets data on the node, or
   * deletes the node.
   *
   * @param path the node path
   * @param watch a boolean to set a watch or not
   * @return Future[GetDataResponse] or Exception
   */
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

  /**
   * Sends a heart beat to the server.
   *
   * @return Future[Unit] or Exception
   */
  protected[this] def ping(): Future[Unit] = {
    val req = ReqPacket(Some(RequestHeader(-2, OpCode.PING)), None)

    connectionManager.connection.get.serve(req) flatMap { rep =>
      if (rep.err.get == 0) Future.Unit
      else Future.exception(
        ZookeeperException.create("Error while ping", rep.err.get))
    }
  }

  /**
   * To set back auth right after reconnection
   *
   * @return Future.Done if request worked, or exception
   */
  private[finagle] def recoverAuth(): Future[Unit] = {
    val fetches = authInfo.toSeq map { auth =>
      val req = ReqPacket(
        Some(RequestHeader(-4, OpCode.AUTH)),
        Some(new AuthRequest(0, auth))
      )
      connectionManager.connection.get.serve(req) flatMap { rep =>
        if (rep.err.get == 0) Future.Unit
        else if (rep.err.get == -115) {
          watchManager.process(
            WatchEvent(Watch.EventType.NONE, Watch.EventState.AUTH_FAILED, ""))
          Future.exception(
            ZookeeperException.create("Error while addAuth", rep.err.get))
        }
        else Future.exception(
          ZookeeperException.create("Error while addAuth", rep.err.get))
      }
    }
    Future.join(fetches)
  }

  /**
   * Set the ACL for the node of the given path if such a node exists and the
   * given version matches the version of the node. Return the stat of the
   * node.
   *
   * @param path the node path
   * @param acl the ACLs to set
   * @param version the node version
   * @return Future[SetACLResponse] or Exception
   */
  def setACL(path: String, acl: Seq[ACL], version: Int): Future[SetACLResponse] = {
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

  /**
   * Set the data for the node of the given path if such a node exists and the
   * given version matches the version of the node (if the given version is
   * -1, it matches any node's versions). Return the stat of the node.
   *
   * This operation, if successful, will trigger all the watches on the node
   * of the given path left by getData calls.
   *
   * The maximum allowable size of the data array is 1 MB (1,048,576 bytes).
   * Arrays larger than this will cause a ZooKeeperException to be thrown.
   *
   * @param path the node path
   * @param data the data Array
   * @param version the node version
   * @return Future[SetDataResponse] or Exception
   */
  def setData(path: String, data: Array[Byte], version: Int): Future[SetDataResponse] = {
    require(data.size < 1048576,
      "The maximum allowable size of the data array is 1 MB (1,048,576 bytes)")

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
   * To set watches back right after reconnection.
   *
   * @return Future[Unit] or Exception
   */
  private[finagle] def setWatches(): Future[Unit] = {
    if (autoWatchReset) {
      val relativeZxid: Long = sessionManager.session.lastZxid.get
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

        connectionManager.connection.get.serve(req) flatMap { rep =>
          if (rep.err.get == 0) Future.Done
          else {
            val exc = ZookeeperException.create(
              "Error while setWatches", rep.err.get)
            ZkClient.logger.error("Error after setting back watches: " + exc.getMessage)
            Future.exception(exc)
          }
        }
      } else
        Future.Done
    } else {
      watchManager.clearWatches()
      Future.Done
    }
  }

  /**
   * Synchronize client and server for a node.
   *
   * @param path the node path
   * @return Future[SyncResponse] or Exception
   */
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

  /**
   * Executes multiple ZooKeeper operations or none of them.
   *
   * On success, a list of results is returned.
   * On failure, an exception is raised which contains partial results and
   * error details.
   * OpRequest:
   * - CheckVersionRequest(path: String, version: Int)
   * - CreateRequest( path: String,
   *                  data: Array[Byte],
   *                  aclList: Seq[ACL],
   *                  createMode: Int )
   * - Create2Request(  path: String,
   *                    data: Array[Byte],
   *                    aclList: Seq[ACL],
   *                    createMode: Int )
   * - DeleteRequest(path: String, version: Int)
   * - SetDataRequest(  path: String,
   *                    data: Array[Byte],
   *                    version: Int )
   *
   * @param opList a Sequence composed of OpRequest
   * @return Future[TransactionResponse] or Exception
   */
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