package com.twitter.finagle.exp.zookeeper.client

import java.util.logging.Logger
import com.twitter.finagle.ServiceFactory
import com.twitter.util.{Await, Future, Time, Closable}
import com.twitter.finagle.exp.zookeeper._
import com.twitter.finagle.exp.zookeeper.ZookeeperDefinitions._

class Client(val factory: ServiceFactory[Request, Response]) extends Closable {

  private[this] val service = Await.result(factory())

  def close(deadline: Time): Future[Unit] = factory.close(deadline)
  def closeService: Future[Unit] = factory.close()

  // Connection purpose definitions
  def connect: Future[Response] = service(new ConnectRequest)
  def connect(timeOut: Int): Future[Response] = service(new ConnectRequest(0, 0L, timeOut))
  def closeSession: Future[Response] = service(new CloseSessionRequest(1, -11))
  def ping: Future[Response] = {
    println("<--ping: ")
    service(new PingRequest(-2, 11))
  }

  def create(path: String,
    data: Array[Byte],
    acl: Array[ACL],
    createMode: Int,
    xid: Int): Future[Response] = {
    //TODO patch check (chroot)
    /* PathUtils.validatePath(path, createMode)
     val finalPath = PathUtils.prependChroot(path, null)*/
    println("<--create: " + xid)
    val req = CreateRequest(xid, opCode.create,path, data, acl, createMode)

    service(req)
  }

  def delete(path: String, version: Int, xid: Int): Future[Unit] = {
    // TODO CHECK STRING
    /*PathUtils.validatePath(path, createMode)
    val finalPath = PathUtils.prependChroot(path, null)*/
    println("<--delete: " + xid)
    val req = DeleteRequest(xid, opCode.delete,path, version)

    service(req).unit
  }

  def exists(path: String, watch: Boolean, xid: Int): Future[Response] = {
    // TODO Check path
    /*PathUtils.validatePath(path, createMode)
    val finalPath = PathUtils.prependChroot(path, null)*/
    println("<--exists: " + xid)
    val req = ExistsRequest(xid, opCode.exists, path, false) // false because watch's not supported

    service(req)
  }

  def getACL(path: String, xid: Int): Future[Response] = {
    // TODO Check path
    /*PathUtils.validatePath(path, createMode)
    val finalPath = PathUtils.prependChroot(path, null)*/
    println("<--getACL: " + xid)
    val req = GetACLRequest(xid, opCode.getACL, path)

    service(req)
  }

  def getChildren(path: String, watch: Boolean, xid: Int): Future[Response] = {
    // TODO Check path
    /*PathUtils.validatePath(path, createMode)
    val finalPath = PathUtils.prependChroot(path, null)*/
    println("<--getChildren: " + xid)
    val req = GetChildrenRequest(xid, opCode.getChildren,path, false) // false because watch's not supported

    service(req)
  }

  def getChildren2(path: String, watch: Boolean, xid: Int): Future[Response] = {
    // TODO Check path
    /*PathUtils.validatePath(path, createMode)
    val finalPath = PathUtils.prependChroot(path, null)*/
    println("<--getChildren2: " + xid)
    val req = GetChildren2Request(xid, opCode.getChildren2, path, false) // false because watch's not supported

    service(req)
  }

  def getData(path: String, watch: Boolean, xid: Int): Future[Response] = {
    // TODO Check path
    /*PathUtils.validatePath(path, createMode)
    val finalPath = PathUtils.prependChroot(path, null)*/
    println("<--getData: " + xid)
    val req = GetDataRequest(xid, opCode.getData, path, false) // false because watch's not supported

    service(req)
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

  def setACL(path: String, acl: Array[ACL], version: Int, xid: Int): Future[Response] = {
    // TODO Check path
    /*PathUtils.validatePath(path, createMode)
    val finalPath = PathUtils.prependChroot(path, null)*/
    println("<--setACL: " + xid)
    val req = SetACLRequest(xid, opCode.setACL, path, acl, version)

    service(req)
  }

  def setData(path: String, data: Array[Byte], version: Int, xid: Int): Future[Response] = {
    // TODO check path
    /*PathUtils.validatePath(path, createMode)
    val finalPath = PathUtils.prependChroot(path, null)*/
    println("<--setData: " + xid)
    val req = SetDataRequest(xid, opCode.setData, path, data, version)

    service(req)
  }

  def setWatches(relativeZxid: Int,
    dataWatches: Array[String],
    existsWatches: Array[String],
    childWatches: Array[String],
    xid: Int
    ): Future[Response] = {
    println("<--setWatches: " + xid)

    val req = SetWatchesRequest(xid, opCode.setWatches, relativeZxid, dataWatches, existsWatches, childWatches)

    service(req)
  }

  def sync(path: String, xid: Int): Future[Response] = {
    // TODO check path
    /*PathUtils.validatePath(path, createMode)
    val finalPath = PathUtils.prependChroot(path, null)*/
    println("<--sync: " + xid)
    val req = SyncRequest(xid, opCode.sync, path)

    service(req)
  }

  def transaction(opList: Array[OpRequest], xid: Int): Future[Response] = {
    println("<--Transaction: " + xid)

    val transaction = new Transaction(opList)
    val req = new TransactionRequest(xid, opCode.multi, transaction)

    service(req)
  }
}

object Client {
  private[this] val logger = Logger.getLogger("finagle-zookeeper")

  def apply(factory: ServiceFactory[Request, Response]): Client = {
    new Client(factory)
  }

  def getLogger = logger
}