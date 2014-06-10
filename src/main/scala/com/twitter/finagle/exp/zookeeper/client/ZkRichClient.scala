package com.twitter.finagle.exp.zookeeper.client

import com.twitter.finagle.exp.zookeeper._
import com.twitter.finagle.exp.zookeeper.data.{ACL, Auth}
import com.twitter.finagle.exp.zookeeper.utils.PathUtils._
import com.twitter.finagle.exp.zookeeper.watch.{WatchType, WatchManager}
import com.twitter.finagle.ServiceFactory
import com.twitter.logging.Logger
import com.twitter.util._

class ZkClient(
  factory: ServiceFactory[Request, Response],
  readOnly: Boolean = false,
  chroot: Option[String] = None
  ) extends Closable {

  private[this] val service = Await.result(factory())
  private[this] val watchManager: WatchManager = new WatchManager(chroot)

  //TODO implement request to the dispatcher (getState, getSessionID, getPassword

  def addAuth(auth: Auth): Future[Unit] = {
    // TODO check auth ?
    service(new AuthRequest(0, auth)).unit
  }

  def addAuth(scheme: String, data: Array[Byte]): Future[Unit] = {
    // TODO check auth ?
    service(new AuthRequest(0, Auth(scheme, data))).unit
  }

  def prepareConnect: Future[Unit] = {
    service(PrepareRequest(watchManager)).unit
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

    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath, createMode)
    ACL.check(acl)
    val req = CreateRequest(finalPath, data, acl, createMode)

    service(req).asInstanceOf[Future[CreateResponse]] map (rep =>
      rep.path.substring(chroot.getOrElse("").length))
  }

  def delete(path: String, version: Int): Future[Unit] = {

    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath)
    val req = DeleteRequest(finalPath, version)

    service(req).unit
  }

  def exists(path: String, watch: Boolean = false): Future[ExistsResponse] = {

    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath)
    val req = ExistsRequest(finalPath, watch)

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

    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath)
    val req = GetACLRequest(finalPath)

    service(req).asInstanceOf[Future[GetACLResponse]]
  }

  def getChildren(path: String, watch: Boolean = false): Future[GetChildrenResponse] = {

    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath)
    val req = GetChildrenRequest(finalPath, watch)

    service(req).asInstanceOf[Future[GetChildrenResponse]] transform {
      case Return(res) =>
        if (watch) {
          val watch = watchManager.register(path, WatchType.exists)
          val childrenList = res.children map (_.substring(chroot.getOrElse("").length))
          val rep = GetChildrenResponse(childrenList, Some(watch))
          Future(rep)
        } else {
          val childrenList = res.children map (_.substring(chroot.getOrElse("").length))
          val rep = GetChildrenResponse(childrenList, None)
          Future(rep)
        }

      case Throw(exc) => Future.exception(exc)
    }
  }

  def getChildren2(path: String, watch: Boolean = false): Future[GetChildren2Response] = {

    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath)
    val req = GetChildren2Request(finalPath, watch)

    service(req).asInstanceOf[Future[GetChildren2Response]] transform {
      case Return(res) =>
        if (watch) {
          val watch = watchManager.register(path, WatchType.exists)
          val childrenList = res.children map (_.substring(chroot.getOrElse("").length))
          val rep = GetChildren2Response(childrenList, res.stat, Some(watch))
          Future(rep)
        } else {
          val childrenList = res.children map (_.substring(chroot.getOrElse("").length))
          val rep = GetChildren2Response(childrenList, res.stat, None)
          Future(rep)
        }

      case Throw(exc) => Future.exception(exc)
    }
  }

  def getData(path: String, watch: Boolean = false): Future[GetDataResponse] = {

    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath)
    val req = GetDataRequest(finalPath, watch)

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

    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath)
    ACL.check(acl)
    val req = SetACLRequest(finalPath, acl, version)

    service(req).asInstanceOf[Future[SetACLResponse]]
  }

  def setData(path: String, data: Array[Byte], version: Int): Future[SetDataResponse] = {

    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath)
    val req = SetDataRequest(finalPath, data, version)

    service(req).asInstanceOf[Future[SetDataResponse]]
  }

  /*  // We can only use this on reconnection
    def setWatches(relativeZxid: Int,
      dataWatches: Array[String],
      existsWatches: Array[String],
      childWatches: Array[String]
      ): Future[Unit] = {
      val req = SetWatchesRequest(relativeZxid, dataWatches, existsWatches, childWatches)
      // fixme chroot all paths
      // fixme add watches in watchManager if request succeed
      service(req).unit
    }*/

  def sync(path: String): Future[SyncResponse] = {

    val finalPath = prependChroot(path, chroot)
    validatePath(finalPath)
    val req = SyncRequest(finalPath)

    service(req).asInstanceOf[Future[SyncResponse]] flatMap { response =>
      val rep = SyncResponse(response.path.substring(chroot.getOrElse("").length))
      Future(rep)
    }
  }

  def transaction(opList: Array[OpRequest]): Future[TransactionResponse] = {

    Transaction.prepareAndCheck(opList, chroot) match {
      case Return(res) =>
        val transaction = new Transaction(res)
        val req = new TransactionRequest(transaction)
        service(req).asInstanceOf[Future[TransactionResponse]] flatMap{ rep =>
          val finalOpList = Transaction.formatPath(rep.responseList, chroot)
          Future(TransactionResponse(finalOpList))
        }

      case Throw(exc) => Future.exception(exc)
    }
  }
}

object ZkClient {
  private[this] val logger = Logger("Finagle-zookeeper")
  def getLogger = logger

  def apply(factory: ServiceFactory[Request, Response]): ZkClient = {
    new ZkClient(factory)
  }
}