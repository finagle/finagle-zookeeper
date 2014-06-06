package com.twitter.finagle.exp.zookeeper.client

import com.twitter.finagle.ServiceFactory
import com.twitter.util._
import com.twitter.finagle.exp.zookeeper._
import com.twitter.logging.Logger
import com.twitter.finagle.exp.zookeeper.data.{ACL, Auth}
import com.twitter.finagle.exp.zookeeper.utils.PathUtils
import com.twitter.finagle.exp.zookeeper.watch.{WatchType, WatchManager}

class ZkClient(
  factory: ServiceFactory[Request, Response],
  readOnly: Boolean = false,
  chroot: Option[String] = None,
  watchManager: WatchManager = new WatchManager
  ) extends Closable {

  private[this] val service = Await.result(factory())
  val logger = ZkClient.getLogger

  //TODO implement request to the dispatcher (getState, getSessionID, getPassword)

  def addAuth(auth: Auth): Future[Unit] = {
    // TODO check auth ?
    service(new AuthRequest(0, auth)).unit
  }

  def addAuth(scheme: String, data: Array[Byte]): Future[Unit] = {
    // TODO check auth ?
    service(new AuthRequest(0, Auth(scheme, data))).unit
  }

  def connect(timeOut: Int = 2000): Future[ConnectResponse] = {
    service(new ConnectRequest(0, 0L, timeOut)).asInstanceOf[Future[ConnectResponse]]
  }

  def close(deadline: Time): Future[Unit] = factory.close(deadline)
  def closeSession(): Future[Unit] = service(new CloseSessionRequest).unit
  def closeService: Future[Unit] = factory.close()

  def create(
    path: String,
    data: Array[Byte],
    acl: Array[ACL],
    createMode: Int): Future[String] = {

    PathUtils.validatePath(path, createMode)
    ACL.check(acl)

    // TODO support chroot
    //val finalPath = PathUtils.prependChroot(path, null)*/
    val req = CreateRequest(path, data, acl, createMode)

    service(req).asInstanceOf[Future[CreateResponse]] map (rep => rep.path)
  }

  def delete(path: String, version: Int): Future[Unit] = {
    PathUtils.validatePath(path)
    // TODO support chroot
    //val finalPath = PathUtils.prependChroot(path, null)*/
    val req = DeleteRequest(path, version)

    service(req).unit
  }

  def exists(path: String, watch: Boolean = false): Future[ExistsResponse] = {
    PathUtils.validatePath(path)
    // TODO support chroot
    require(path.length != 0, "Path must be longer than 0")

    //val finalPath = PathUtils.prependChroot(path, null)*/
    val req = ExistsRequest(path, watch)

    service(req).asInstanceOf[Future[NodeWithWatch]] transform {
      case Return(res) =>
        if (watch) {
          val watch = watchManager.register(path, WatchType.exists)
          val rep = NodeWithWatch(res.stat, Some(watch))
          Future(rep)
        } else {
          Future(res)
        }


      case Throw(exc) => exc match {
        case exc: NoNodeException =>
          if (watch) {
            val watch = watchManager.register(path, WatchType.exists)
            Future(NoNodeWatch(watch))
          } else {
            Future.exception(exc)
          }

        case _ => Future.exception(exc)
      }
    }
  }

  def getACL(path: String): Future[GetACLResponse] = {
    PathUtils.validatePath(path)
    // TODO support chroot
    require(path.length != 0, "Path must be longer than 0")
    //val finalPath = PathUtils.prependChroot(path, null)*/
    val req = GetACLRequest(path)

    service(req).asInstanceOf[Future[GetACLResponse]]
  }

  def getChildren(path: String, watch: Boolean = false): Future[GetChildrenResponse] = {
    PathUtils.validatePath(path)
    // TODO support chroot
    require(path.length != 0, "Path must be longer than 0")

    //val finalPath = PathUtils.prependChroot(path, null)*/
    val req = GetChildrenRequest(path, watch)

    service(req).asInstanceOf[Future[GetChildrenResponse]] transform {
      case Return(res) =>
        if (watch) {
          val watch = watchManager.register(path, WatchType.exists)
          val rep = GetChildrenResponse(res.children, Some(watch))
          Future(rep)
        } else {
          Future(res)
        }

      case Throw(exc) => Future.exception(exc)
    }
  }

  def getChildren2(path: String, watch: Boolean = false): Future[GetChildren2Response] = {
    PathUtils.validatePath(path)
    // TODO support chroot
    //val finalPath = PathUtils.prependChroot(path, null)*/
    val req = GetChildren2Request(path, watch)

    service(req).asInstanceOf[Future[GetChildren2Response]] transform {
      case Return(res) =>
        if (watch) {
          val watch = watchManager.register(path, WatchType.exists)
          val rep = GetChildren2Response(res.children, res.stat, Some(watch))
          Future(rep)
        } else {
          Future(res)
        }

      case Throw(exc) => Future.exception(exc)
    }
  }

  def getData(path: String, watch: Boolean = false): Future[GetDataResponse] = {
    PathUtils.validatePath(path)
    // TODO support chroot
    //val finalPath = PathUtils.prependChroot(path, null)*/
    val req = GetDataRequest(path, watch)

    service(req).asInstanceOf[Future[GetDataResponse]] transform {
      case Return(res) =>
        if (watch) {
          val watch = watchManager.register(path, WatchType.exists)
          val rep = GetDataResponse(res.data, res.stat, Some(watch))
          Future(rep)
        } else {
          Future(res)
        }

      case Throw(exc) => Future.exception(exc)
    }
  }

  // GetMaxChildren is implemented but not available in the java lib
  /*def getMaxChildren(path: String, xid: Int): Future[Response] = {
    /*PathUtils.validatePath(path, createMode)
    val finalPath = PathUtils.prependChroot(path, null)*/
    println("<--getMaxChildren: " + xid)

    val header = RequestHeader(xid, ?)
    val body = GetDataRequestBody(path, false) // false because watch's not supported

    service(new GetDataRequest(header, body))
  }*/

  def setACL(path: String, acl: Array[ACL], version: Int): Future[SetACLResponse] = {
    // TODO support chroot
    PathUtils.validatePath(path)
    ACL.check(acl)
    //val finalPath = PathUtils.prependChroot(path, null)*/
    val req = SetACLRequest(path, acl, version)

    service(req).asInstanceOf[Future[SetACLResponse]]
  }

  def setData(path: String, data: Array[Byte], version: Int): Future[SetDataResponse] = {
    // TODO support chroot
    PathUtils.validatePath(path)
    //val finalPath = PathUtils.prependChroot(path, null)*/
    val req = SetDataRequest(path, data, version)

    service(req).asInstanceOf[Future[SetDataResponse]]
  }

  // We can only use this on reconnection
  def setWatches(relativeZxid: Int,
    dataWatches: Array[String],
    existsWatches: Array[String],
    childWatches: Array[String]
    ): Future[Unit] = {
    val req = SetWatchesRequest(relativeZxid, dataWatches, existsWatches, childWatches)
    // fixme add watches in watchManager if request succeed
    service(req).unit
  }

  def sync(path: String): Future[SyncResponse] = {
    PathUtils.validatePath(path)
    // TODO support chroot
    //val finalPath = PathUtils.prependChroot(path, null)*/
    val req = SyncRequest(path)

    service(req).asInstanceOf[Future[SyncResponse]]
  }

  def transaction(opList: Array[OpRequest]): Future[TransactionResponse] = {
    // TODO check each operation (path, chroot, acl)
    // TODO use PathUtils to check path
    // TODO support chroot
    // TODO check ACL
    val transaction = new Transaction(opList)
    val req = new TransactionRequest(transaction)

    service(req).asInstanceOf[Future[TransactionResponse]]
  }
}

object ZkClient {
  private[this] val logger = Logger("Finagle-zookeeper")
  def getLogger = logger

  def apply(factory: ServiceFactory[Request, Response]): ZkClient = {
    new ZkClient(factory)
  }
}