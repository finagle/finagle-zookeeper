package com.twitter.finagle.exp.zookeeper.client

import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.OpCode
import com.twitter.finagle.exp.zookeeper._
import com.twitter.finagle.exp.zookeeper.client.managers.ClientManager
import com.twitter.finagle.exp.zookeeper.connection.{ConnectionManager, HostUtilities}
import com.twitter.finagle.exp.zookeeper.data.{ACL, Auth, Stat}
import com.twitter.finagle.exp.zookeeper.session.{Session, SessionManager}
import com.twitter.finagle.exp.zookeeper.utils.PathUtils
import com.twitter.finagle.exp.zookeeper.utils.PathUtils._
import com.twitter.finagle.exp.zookeeper.watcher.Watch.{WatcherMapType, WatcherType}
import com.twitter.finagle.exp.zookeeper.watcher.{Watch, Watcher, WatcherManager}
import com.twitter.logging.{Level, Logger}
import com.twitter.util._

trait ZkClient extends Closable with ClientManager {
  val dest: String
  val label: Option[String]
  val canReadOnly: Boolean
  val chroot: String
  val sessionTimeout: Duration
  val autoWatchReset: Boolean
  val autoReconnect: Boolean
  val timeBetweenRwSrch: Option[Duration]
  val timeBetweenPrevSrch: Option[Duration]
  val maxConsecutiveRetries: Int
  val maxReconnectAttempts: Int
  val timeBetweenAttempts: Duration
  val timeBetweenLinkCheck: Option[Duration]

  private[finagle] lazy val connectionManager =
    new ConnectionManager(
      dest,
      label,
      canReadOnly,
      timeBetweenPrevSrch,
      timeBetweenRwSrch)

  private[finagle] lazy val sessionManager = new SessionManager(canReadOnly)
  lazy val watcherManager: WatcherManager =
    new WatcherManager(chroot, autoWatchReset)
  private[finagle] lazy val zkRequestService =
    new PreProcessService(connectionManager, sessionManager, this)

  @volatile protected[this] var authInfo: Set[Auth] = Set()

  def session: Session = sessionManager.session

  /**
   * Add the specified Auth(scheme:data) information to this connection.
   *
   * @param scheme authentication scheme
   * @param data corresponding data for this node
   * @return Future[Unit] or Exception
   */
  def addAuth(scheme: String, data: Array[Byte]): Future[Unit] = {
    val auth = Auth(scheme, data)
    val req = new AuthRequest(0, auth)

    zkRequestService(req) flatMap { rep =>
      if (rep.err.get == 0) {
        authInfo += auth
        Future.Unit
      } else if (rep.err.get == -115) {
        watcherManager.process(
          WatchEvent(Watch.EventType.NONE, Watch.EventState.AUTH_FAILED, "")
        )
        Future.exception(
          ZookeeperException.create("Error while addAuth", rep.err.get)
        )
      }
      else Future.exception(
        ZookeeperException.create("Error while addAuth", rep.err.get)
      )
    }
  }

  /**
   * Checks if the watches on a znode for a specified watcher type are still
   * available on the server side.
   * Not released until v3.5.0
   *
   * @param path the path to the znode
   * @param watcherType the watcher type (children, data, any)
   * @return Future[Unit] if the watches are ok, or else ZooKeeperException
   * @since 3.5.0
   */
  private[this] def checkWatches(
    path: String,
    watcherType: Int
  ): Future[Unit] = {
    if (!watcherManager.isWatcherDefined(path, watcherType))
      throw new IllegalArgumentException("No watch registered for this node")

    PathUtils.validatePath(path)
    val finalPath = prependChroot(path, chroot)
    val req = CheckWatchesRequest(finalPath, watcherType)

    zkRequestService(req) flatMap { rep =>
      if (rep.err.get == 0) Future.Unit
      else Future.exception(
        ZookeeperException.create("Error while removing watches", rep.err.get)
      )
    }
  }

  /**
   * Checks if a watcher event is still not triggered.
   * Not released until v3.5.0
   *
   * @param watcher a watcher to test
   * @return Future[Unit] if the watcher is ok, or else ZooKeeperException
   */
  private[this] def checkWatcher(watcher: Watcher): Future[Unit] = {
    if (!watcherManager.isWatcherDefined(watcher))
      throw new IllegalArgumentException("No watch registered for this node")

    PathUtils.validatePath(watcher.path)
    val finalPath = prependChroot(watcher.path, chroot)
    val watcherType = watcher.typ match {
      case WatcherMapType.data | WatcherMapType.exists => WatcherType.DATA
      case WatcherMapType.children => WatcherType.CHILDREN
    }
    val req = CheckWatchesRequest(finalPath, watcherType)

    zkRequestService(req) flatMap { rep =>
      if (rep.err.get == 0) Future.Unit
      else Future.exception(
        ZookeeperException.create("Error while removing watches", rep.err.get)
      )
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
  def close(deadline: Time): Future[Unit] =
    stopJob() ensure connectionManager.close(deadline)

  /**
   * Closes the session and stops background jobs.
   *
   * @return Future[Unit] or Exception
   */
  def closeSession(): Future[Unit] = disconnect()

  /**
   * We use this to configure the dispatcher, gives connectionManager,
   * WatchManager and SessionManager.
   *
   * @return Future.Done
   */
  private[finagle] def configureDispatcher(): Future[Unit] = {
    val req = ReqPacket(None, Some(ConfigureRequest(
      connectionManager,
      sessionManager,
      watcherManager
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
    acl: Seq[ACL],
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
        ZookeeperException.create(
          s"Error while create for path: $path", rep.err.get
        )
      )
    }
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
   * @return Future[Create2Response] or Exception
   * @since 3.5.0
   */
  def create2(
    path: String,
    data: Array[Byte],
    acl: Seq[ACL],
    createMode: Int): Future[Create2Response] = {
    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath, createMode)
    ACL.check(acl)
    val req = Create2Request(finalPath, data, acl, createMode)

    zkRequestService(req) flatMap { rep =>
      if (rep.err.get == 0) {
        val finalRep = rep.response.get.asInstanceOf[Create2Response]
        val lastPath = finalRep.path.substring(chroot.length)
        Future(Create2Response(lastPath, finalRep.stat))
      } else Future.exception(
        ZookeeperException.create(
          s"Error while create2 for path: $path", rep.err.get
        )
      )
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
        ZookeeperException.create(
          s"Error while delete for path: $path", rep.err.get
        )
      )
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
   * @return an ExistsReponse
   */
  def exists(path: String, watch: Boolean = false): Future[ExistsResponse] = {
    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath)
    val req = ExistsRequest(finalPath, watch)

    zkRequestService(req) flatMap { rep =>
      rep.response match {
        case Some(response: ExistsResponse) =>
          if (watch) {
            val watcher = watcherManager.registerWatcher(
              path,
              Watch.WatcherMapType.exists
            )
            val finalRep = ExistsResponse(response.stat, Some(watcher))
            Future(finalRep)
          } else Future(response)

        case None =>
          if (rep.err.get == -101 && watch) {
            val watcher = watcherManager.registerWatcher(
              path,
              Watch.WatcherMapType.exists
            )
            Future(ExistsResponse(None, Some(watcher)))
          } else Future.exception(
            ZookeeperException.create(
              s"Error while exists for path: $path", rep.err.get
            )
          )

        case _ =>
          Future.exception(
            ZookeeperException.create("Match error while exists")
          )
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
        ZookeeperException.create(
          s"Error while getACL for path: $path", rep.err.get
        )
      )
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
  def getChildren(
    path: String,
    watch: Boolean = false
  ): Future[GetChildrenResponse] = {
    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath)
    val req = GetChildrenRequest(finalPath, watch)

    zkRequestService(req) flatMap { rep =>
      if (rep.err.get == 0) {
        val res = rep.response.get.asInstanceOf[GetChildrenResponse]
        if (watch) {
          val watcher = watcherManager.registerWatcher(
            path,
            Watch.WatcherMapType.children
          )
          val finalRep = GetChildrenResponse(res.children, Some(watcher))
          Future(finalRep)
        }
        else Future(rep.response.get.asInstanceOf[GetChildrenResponse])
      }
      else Future.exception(
        ZookeeperException.create(
          s"Error while getChildren for path: $path", rep.err.get
        )
      )
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
  def getChildren2(
    path: String,
    watch: Boolean = false): Future[GetChildren2Response] = {
    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath)
    val req = GetChildren2Request(finalPath, watch)

    zkRequestService(req) flatMap { rep =>
      if (rep.err.get == 0) {
        val res = rep.response.get.asInstanceOf[GetChildren2Response]
        if (watch) {
          val watcher = watcherManager.registerWatcher(
            path,
            Watch.WatcherMapType.children
          )
          val finalRep = GetChildren2Response(
            res.children,
            res.stat, Some(watcher)
          )
          Future(finalRep)
        }
        else Future(rep.response.get.asInstanceOf[GetChildren2Response])
      }
      else Future.exception(
        ZookeeperException.create(
          s"Error while getChildren2 for path: $path", rep.err.get
        )
      )
    }
  }

  /**
   * Return the last committed configuration (as known to the server to which
   * the client is connected) and the stat of the configuration.
   * Not released until v3.5.0
   *
   * If the watch is true and the call is successful (no exception is
   * thrown), a watch will be left on the configuration node. The watch
   * will be triggered by a successful reconfig operation.
   *
   * @param watch set a watch or not
   * @return configuration node data
   * @since 3.5.0
   */
  private[this] def getConfig(
    watch: Boolean = false
  ): Future[GetDataResponse] = {
    val req = GetDataRequest(ZookeeperDefs.CONFIG_NODE, watch)

    zkRequestService(req) flatMap { rep =>
      if (rep.err.get == 0) {
        val res = rep.response.get.asInstanceOf[GetDataResponse]
        if (watch) {
          val watch = watcherManager.registerWatcher(
            ZookeeperDefs.CONFIG_NODE,
            Watch.WatcherMapType.data
          )
          val finalRep = GetDataResponse(res.data, res.stat, Some(watch))
          Future(finalRep)
        }
        else Future(res)
      }
      else Future.exception(
        ZookeeperException.create("Error while getConfig", rep.err.get)
      )
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
  def getData(
    path: String,
    watch: Boolean = false
  ): Future[GetDataResponse] = {
    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath)
    val req = GetDataRequest(finalPath, watch)

    zkRequestService(req) flatMap { rep =>
      if (rep.err.get == 0) {
        val res = rep.response.get.asInstanceOf[GetDataResponse]
        if (watch) {
          val watcher = watcherManager.registerWatcher(
            path,
            Watch.WatcherMapType.data
          )
          val finalRep = GetDataResponse(res.data, res.stat, Some(watcher))
          Future(finalRep)
        }
        else Future(res)
      }
      else Future.exception(
        ZookeeperException.create(
          s"Error while getData for path: $path", rep.err.get
        )
      )
    }
  }

  /**
   * Sends a heart beat to the server.
   *
   * @return Future[Unit] or Exception
   */
  private[finagle] def ping(): Future[Unit] = {
    val req = ReqPacket(Some(RequestHeader(-2, OpCode.PING)), None)

    connectionManager.connection.get.serve(req) flatMap { rep =>
      if (rep.err.get == 0) Future.Unit
      else Future.exception(
        ZookeeperException.create("Error while ping", rep.err.get)
      )
    }
  }

  /**
   * Reconfigure - add/remove servers. Return the new configuration.
   * Not released until v3.5.0
   *
   * @param joiningServers a comma separated list of servers being added
   *                       (incremental reconfiguration)
   * @param leavingServers a comma separated list of servers being removed
   *                       (incremental reconfiguration)
   * @param newMembers a comma separated list of new membership
   *                   (non-incremental reconfiguration)
   * @param fromConfig version of the current configuration (optional -
   *                   causes reconfiguration to throw an exception if
   *                   configuration is no longer current)
   * @return configuration node data
   * @since 3.5.0
   */
  private[this] def reconfig(
    joiningServers: String,
    leavingServers: String,
    newMembers: String,
    fromConfig: Long
  ): Future[GetDataResponse] = {
    HostUtilities.formatAndTest(joiningServers)
    HostUtilities.formatAndTest(leavingServers)
    HostUtilities.formatAndTest(newMembers)
    val req = ReconfigRequest(
      joiningServers,
      leavingServers,
      newMembers,
      fromConfig
    )

    zkRequestService(req) flatMap { rep =>
      if (rep.err.get == 0) {
        val res = rep.response.get.asInstanceOf[GetDataResponse]
        Future(res)
      }
      else Future.exception(
        ZookeeperException.create("Error while reconfig", rep.err.get)
      )
    }
  }

  /**
   * To set back auth right after reconnection.
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
          watcherManager.process(
            WatchEvent(Watch.EventType.NONE, Watch.EventState.AUTH_FAILED, "")
          )
          Future.exception(
            ZookeeperException.create("Error while addAuth", rep.err.get))
        }
        else Future.exception(
          ZookeeperException.create("Error while addAuth", rep.err.get)
        )
      }
    }
    Future.join(fetches)
  }

  /**
   * Convenience method to remove a watcher or all watchers on a znode.
   *
   * @param opCode CHECK_WATCHES or REMOVE_WATCHES
   * @param path the node path
   * @param watcherType the watcher type (children, data, any)
   * @param local whether watches can be removed locally when there is no
   *              server connection.
   * @return Future[Unit]
   * @since 3.5.0
   */
  private[this] def removeWatches(
    opCode: Int,
    path: String,
    watcherType: Int,
    local: Boolean
  ): Future[RepPacket] = {
    PathUtils.validatePath(path)
    val finalPath = prependChroot(path, chroot)

    val req = opCode match {
      case OpCode.CHECK_WATCHES =>
        CheckWatchesRequest(finalPath, watcherType)
      case OpCode.REMOVE_WATCHES =>
        RemoveWatchesRequest(finalPath, watcherType)
    }

    zkRequestService(req)
  }

  /**
   * For the given znode path, removes the specified watcher.
   * Not released until v3.5.0
   *
   * @param watcher the watcher to be removed
   * @param local whether watches can be removed locally when there is no
   *              server connection.
   * @return Future[Unit]
   * @since 3.5.0
   */
  private[this] def removeWatches(
    watcher: Watcher,
    local: Boolean
  ): Future[Unit] = {
    if (!watcherManager.isWatcherDefined(watcher))
      throw new IllegalArgumentException("No watch registered for this node")

    val watcherType = watcher.typ match {
      case WatcherMapType.data | WatcherMapType.exists => WatcherType.DATA
      case WatcherMapType.children => WatcherType.CHILDREN
    }

    removeWatches(
      OpCode.CHECK_WATCHES,
      watcher.path,
      watcherType,
      local
    ) flatMap { rep =>
      if (rep.err.get == 0) {
        watcherManager.removeWatcher(watcher)
        Future.Unit
      }
      else {
        if (local) {
          watcherManager.removeWatcher(watcher)
          Future.Done
        }
        else Future.exception(
          ZookeeperException.create("Error while removing watches", rep.err.get)
        )
      }
    }
  }

  /**
   * For the given znode path, removes all the registered watchers of given
   * watcherType. Not released until v3.5.0
   *
   * @param path the node path
   * @param watcherType the watcher type (children, data, any)
   * @param local whether watches can be removed locally when there is no
   *              server connection.
   * @return Future[Unit]
   * @since 3.5.0
   */
  private[this] def removeAllWatches(
    path: String,
    watcherType: Int,
    local: Boolean
  ): Future[Unit] =
    removeWatches(
      OpCode.REMOVE_WATCHES,
      path,
      watcherType,
      local
    ) flatMap { rep =>
      if (rep.err.get == 0) {
        watcherManager.removeWatchers(path, watcherType)
        Future.Unit
      }
      else {
        if (local) {
          watcherManager.removeWatchers(path, watcherType)
          Future.Done
        }
        else Future.exception(
          ZookeeperException.create(
            "Error while removing watches",
            rep.err.get
          )
        )
      }
    }

  /**
   * Set the ACL for the node of the given path if such a node exists and the
   * given version matches the version of the node. Return the stat of the
   * node.
   *
   * @param path the node path
   * @param acl the ACLs to set
   * @param version the node version
   * @return the new znode stat
   */
  def setACL(path: String, acl: Seq[ACL], version: Int): Future[Stat] = {
    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath)
    ACL.check(acl)
    val req = SetACLRequest(finalPath, acl, version)

    zkRequestService(req) flatMap {
      rep =>
        if (rep.err.get == 0) {
          val res = rep.response.get.asInstanceOf[SetACLResponse]
          Future(res.stat)
        }
        else Future.exception(
          ZookeeperException.create(
            s"Error while setACL for path: $path",
            rep.err.get
          )
        )
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
   * @return the new znode stat
   */
  def setData(path: String, data: Array[Byte], version: Int): Future[Stat] = {
    require(data.size < 1048576,
      "The maximum allowable size of the data array is 1 MB (1,048,576 bytes)")

    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath)
    val req = SetDataRequest(finalPath, data, version)

    zkRequestService(req) flatMap { rep =>
      if (rep.err.get == 0) {
        val res = rep.response.get.asInstanceOf[SetDataResponse]
        Future(res.stat)
      }
      else Future.exception(
        ZookeeperException.create(
          s"Error while setData for path: $path",
          rep.err.get
        )
      )
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
      val dataWatches: Seq[String] =
        watcherManager.getDataWatchers.keySet.map { path =>
          prependChroot(path, chroot)
        }.toSeq

      val existsWatches: Seq[String] =
        watcherManager.getExistsWatchers.keySet.map { path =>
          prependChroot(path, chroot)
        }.toSeq

      val childWatches: Seq[String] =
        watcherManager.getChildrenWatchers.keySet.map { path =>
          prependChroot(path, chroot)
        }.toSeq

      if (dataWatches.nonEmpty
        || existsWatches.nonEmpty
        || childWatches.nonEmpty) {
        val req = ReqPacket(
          Some(RequestHeader(-8, OpCode.SET_WATCHES)),
          Some(
            SetWatchesRequest(
              relativeZxid,
              dataWatches,
              existsWatches,
              childWatches
            )
          )
        )

        connectionManager.connection.get.serve(req) flatMap { rep =>
          if (rep.err.get == 0) Future.Done
          else {
            val exc =
              ZookeeperException.create("Error while setWatches", rep.err.get)
            ZkClient.logger.error(
              s"Error after setting back watches: ${ exc.getMessage }"
            )
            Future.exception(exc)
          }
        }
      }
      else Future.Done
    }
    else {
      watcherManager.clearWatchers()
      Future.Done
    }
  }

  /**
   * Synchronize client and server for a node.
   *
   * @param path the node path
   * @return the synchronized znode's path
   */
  def sync(path: String): Future[String] = {
    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath)
    val req = SyncRequest(finalPath)

    zkRequestService(req) flatMap { rep =>
      if (rep.err.get == 0) {
        val res = rep.response.get.asInstanceOf[SyncResponse]
        val finalRep = SyncResponse(res.path.substring(chroot.length))
        Future(finalRep.path)
      }
      else Future.exception(
        ZookeeperException.create(
          s"Error while sync for path: $path",
          rep.err.get
        )
      )
    }
  }

  /**
   * Executes multiple ZooKeeper operations or none of them.
   *
   * On success, a list of results is returned.
   * On failure, an exception is raised which contains partial results and
   * error details.
   * OpRequest:
   * - CheckVersionRequest
   * - CreateRequest
   * - Create2Request (available in v3.5.0)
   * - DeleteRequest
   * - SetDataRequest
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
            }
            else Future.exception(
              ZookeeperException.create("Error while transaction", rep.err.get)
            )
        }
      case Throw(exc) => Future.exception(exc)
    }
  }
}

object ZkClient {
  def apply(
    autowatchReset: Boolean,
    autoRecon: Boolean,
    canBeReadOnly: Boolean,
    chrootPath: String,
    hostList: String,
    maxConsecRetries: Int,
    maxReconAttempts: Int,
    serviceLabel: Option[String],
    sessTimeout: Duration,
    timeBtwnAttempts: Duration,
    timeBtwnLinkCheck: Option[Duration],
    timeBtwnRwSrch: Option[Duration],
    timeBtwnPrevSrch: Option[Duration]
  ): ZkClient = new ZkClient {
    override val dest: String = hostList
    val label: Option[String] = serviceLabel
    val autoReconnect: Boolean = autoRecon
    val autoWatchReset: Boolean = autowatchReset
    val canReadOnly: Boolean = canBeReadOnly
    val chroot: String = chrootPath
    val maxConsecutiveRetries: Int = maxConsecRetries
    val maxReconnectAttempts: Int = maxReconAttempts
    val sessionTimeout: Duration = sessTimeout
    val timeBetweenRwSrch: Option[Duration] = timeBtwnRwSrch
    val timeBetweenAttempts: Duration = timeBtwnAttempts
    val timeBetweenLinkCheck: Option[Duration] = timeBtwnLinkCheck
    val timeBetweenPrevSrch: Option[Duration] = timeBtwnPrevSrch
  }

  private[finagle] val logger = Logger("finagle-ZooKeeper")
  logger.setLevel(Level.WARNING)
}