package com.twitter.finagle.exp.zookeeper.client

import com.twitter.finagle.ServiceFactory
import com.twitter.util._
import com.twitter.finagle.exp.zookeeper._
import com.twitter.logging.Logger

class Client(val factory: ServiceFactory[Request, Response]) extends Closable {

  private[this] val service = Await.result(factory())
  val logger = Client.getLogger

  def close(deadline: Time): Future[Unit] = factory.close(deadline)
  def closeService: Future[Unit] = factory.close()

  def connect(timeOut: Int = 2000): Future[ConnectResponse] = {
    service(new ConnectRequest(0, 0L, timeOut)).asInstanceOf[Future[ConnectResponse]]
  }
  def closeSession: Future[Unit] = service(new CloseSessionRequest).unit
  def ping: Future[Unit] = {
    println("<--ping: ")
    service(new PingRequest).unit
  }

  def create(path: String,
    data: Array[Byte],
    acl: Array[ACL],
    createMode: Int): Future[CreateResponse] = {

    require(path.length != 0, "Path must be longer than 0")
    require(acl.size != 0, "ACL list must not be empty")
    require(createMode == 0 || createMode == 1 ||
      createMode == 2 || createMode == 3, "Create mode must be a value [0-3]")

    //TODO patch check (chroot)
    /* PathUtils.validatePath(path, createMode)
     val finalPath = PathUtils.prependChroot(path, null)*/
    val req = CreateRequest(path, data, acl, createMode)

    service(req).asInstanceOf[Future[CreateResponse]]
  }

  def delete(path: String, version: Int): Future[Unit] = {
    // TODO CHECK STRING
    require(path.length != 0, "Path must be longer than 0")
    /*PathUtils.validatePath(path, createMode)
    val finalPath = PathUtils.prependChroot(path, null)*/
    val req = DeleteRequest(path, version)

    service(req).unit
  }

  def exists(path: String, watch: Boolean): Future[ExistsResponse] = {
    // TODO Check path
    require(path.length != 0, "Path must be longer than 0")
    //require(watcher || !watcher, "Watch must be true or false")

    /*PathUtils.validatePath(path, createMode)
    val finalPath = PathUtils.prependChroot(path, null)*/
    val req = ExistsRequest(path, watch)

    service(req).asInstanceOf[Future[ExistsResponse]]
  }

  def getACL(path: String): Future[GetACLResponse] = {
    // TODO Check path
    require(path.length != 0, "Path must be longer than 0")
    /*PathUtils.validatePath(path, createMode)
    val finalPath = PathUtils.prependChroot(path, null)*/
    val req = GetACLRequest(path)

    service(req).asInstanceOf[Future[GetACLResponse]]
  }

  def getChildren(path: String, watch: Boolean): Future[GetChildrenResponse] = {
    // TODO Check path
    require(path.length != 0, "Path must be longer than 0")
    require(watch || !watch, "Watch must be true or false")

    /*PathUtils.validatePath(path, createMode)
    val finalPath = PathUtils.prependChroot(path, null)*/
    val req = GetChildrenRequest(path, watch)

    service(req).asInstanceOf[Future[GetChildrenResponse]]
  }

  def getChildren2(path: String, watch: Boolean): Future[GetChildren2Response] = {
    // TODO Check path
    require(path.length != 0, "Path must be longer than 0")
    require(watch || !watch, "Watch must be true or false")
    /*PathUtils.validatePath(path, createMode)
    val finalPath = PathUtils.prependChroot(path, null)*/
    val req = GetChildren2Request(path, watch)

    service(req).asInstanceOf[Future[GetChildren2Response]]
  }

  def getData(path: String, watch: Boolean): Future[GetDataResponse] = {
    // TODO Check path
    require(path.length != 0, "Path must be longer than 0")
    //require(watcher || !watcher, "Watch must be true or false")
    /*PathUtils.validatePath(path, createMode)
    val finalPath = PathUtils.prependChroot(path, null)*/
    val req = GetDataRequest(path, watch)

    service(req).asInstanceOf[Future[GetDataResponse]]
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
    // TODO Check path
    require(path.length != 0, "Path must be longer than 0")
    require(acl.size != 0, "ACL list must not be empty")
    /*PathUtils.validatePath(path, createMode)
    val finalPath = PathUtils.prependChroot(path, null)*/
    val req = SetACLRequest(path, acl, version)

    service(req).asInstanceOf[Future[SetACLResponse]]
  }

  def setData(path: String, data: Array[Byte], version: Int): Future[SetDataResponse] = {
    // TODO check path
    require(path.length != 0, "Path must be longer than 0")
    /*PathUtils.validatePath(path, createMode)
    val finalPath = PathUtils.prependChroot(path, null)*/
    val req = SetDataRequest(path, data, version)

    service(req).asInstanceOf[Future[SetDataResponse]]
  }

  def setWatches(relativeZxid: Int,
    dataWatches: Array[String],
    existsWatches: Array[String],
    childWatches: Array[String]
    ): Future[Unit] = {
    val req = SetWatchesRequest(relativeZxid, dataWatches, existsWatches, childWatches)

    service(req).unit
  }

  def sync(path: String): Future[SyncResponse] = {
    // TODO check path
    require(path.length != 0, "Path must be longer than 0")
    /*PathUtils.validatePath(path, createMode)
    val finalPath = PathUtils.prependChroot(path, null)*/
    val req = SyncRequest(path)

    service(req).asInstanceOf[Future[SyncResponse]]
  }

  def transaction(opList: Array[OpRequest]): Future[TransactionResponse] = {

    val transaction = new Transaction(opList)
    val req = new TransactionRequest(transaction)

    service(req).asInstanceOf[Future[TransactionResponse]]
  }
}

object Client {
  private[this] val logger = Logger("Finagle-zookeeper")
  def getLogger = logger

  def apply(factory: ServiceFactory[Request, Response]): Client = {
    new Client(factory)
  }
}