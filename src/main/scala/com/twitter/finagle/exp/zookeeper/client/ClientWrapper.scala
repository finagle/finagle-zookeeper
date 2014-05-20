package com.twitter.finagle.exp.zookeeper.client

import com.twitter.util.{Throw, Return, Try, Future}
import com.twitter.conversions.time._
import com.twitter.finagle.exp.zookeeper._

/**
 * ClientWrapper (can also be named SessionManager) is used as a Wrapper around Client,
 * it allows to check ReplyHeader, this way we can check the connection state, zxid,
 * error types.
 *
 * For example with the Connect command, client.connect is called,
 * the result is then flatMapped, so that we can apply transformation
 * to a Future value. The ConnectResponse is parsed by the connection manager,
 * this way we know different session variables, and we can launch the ping timer
 *
 * The ping timer is a scheduler based on DefaultTimer which sends a ping request
 * to the server every X milliseconds to keep the connection alive
 *
 * The connectionManager is suppose to check the connection state and try to
 * reconnect if the connection is lost.
 */

object ClientWrapper {

  def newClient(adress: String, timeOut: Long): ClientWrapper = {
    new ClientWrapper(adress, timeOut)
  }
}

case class ClientWrapper(adress: String, timeOut: Long) {
  val connectionManager = new ConnexionManager
  val client = ZooKeeper.newRichClient(adress)
  val pingTimer = new PingTimer
  val logger = Client.getLogger

  def connect: Future[Option[ConnectResponse]] = {
    // flatMap client.connect to get Future[BufferedResponse]
    client.connect flatMap { rep =>
      val pureRep = rep.asInstanceOf[ConnectResponse]
      connectionManager.parseConnectResponse(pureRep)
      // We can start to send ping to keep session alive
      pingTimer(connectionManager.realTimeout.milliseconds)(sendPing)
      Future.value(Some(pureRep))
    }
  }

  def create(path: String,
    data: Array[Byte],
    acl: Array[ACL],
    createMode: Int
    ): Future[Option[CreateResponseBody]] = {

    require(path.length != 0, "Path must be longer than 0")
    require(acl.size != 0, "ACL list must not be empty")
    require(createMode == 0 || createMode == 1 ||
      createMode == 2 || createMode == 3, "Create mode must be a value [0-3]")

    client.create(path, data, acl, createMode, connectionManager.getXid) flatMap {
      rep =>
        val pureRep = rep.asInstanceOf[CreateResponse]
        parseCreate(pureRep)
        Future.value(pureRep.body)
    }
  }

  def delete(path: String, version: Int): Future[Option[ReplyHeader]] = {
    require(path.length != 0, "Path must be longer than 0")

    client.delete(path, version, connectionManager.getXid) flatMap {
      rep =>
        val pureRep = rep.asInstanceOf[ReplyHeader]
        parseDelete(pureRep)
        Future.value(Some(pureRep))
    }
  }

  def disconnect: Future[Option[ReplyHeader]] = {
    pingTimer.stopTimer
    client.closeSession flatMap { rep =>
      val pureRep = rep.asInstanceOf[ReplyHeader]
      connectionManager.parseReplyHeader(pureRep)
      Future.value(Some(pureRep))
    }
  }

  def exists(path: String, watcher: Boolean): Future[Option[ExistsResponseBody]] = {
    require(path.length != 0, "Path must be longer than 0")
    require(watcher || !watcher, "Watch must be true or false")

    client.exists(path, watcher, connectionManager.getXid) flatMap {
      rep =>
        val pureRep = rep.asInstanceOf[ExistsResponse]
        parseExists(pureRep)
        Future.value(pureRep.body)
    }
  }

  def getACL(path: String): Future[Option[GetACLResponseBody]] = {
    require(path.length != 0, "Path must be longer than 0")

    client.getACL(path, connectionManager.getXid) flatMap {
      rep =>
        val pureRep = rep.asInstanceOf[GetACLResponse]
        parseGetACL(pureRep)
        Future.value(pureRep.body)
    }
  }

  def getChildren(path: String, watch: Boolean): Future[Option[GetChildrenResponseBody]] = {
    require(path.length != 0, "Path must be longer than 0")
    require(watch || !watch, "Watch must be true or false")

    client.getChildren(path, watch, connectionManager.getXid) flatMap {
      rep =>
        val pureRep = rep.asInstanceOf[GetChildrenResponse]
        parseGetChildren(pureRep)
        Future.value(pureRep.body)
    }
  }

  def getChildren2(path: String, watch: Boolean): Future[Option[GetChildren2ResponseBody]] = {
    require(path.length != 0, "Path must be longer than 0")
    require(watch || !watch, "Watch must be true or false")

    client.getChildren2(path, watch, connectionManager.getXid) flatMap {
      rep =>
        val pureRep = rep.asInstanceOf[GetChildren2Response]
        parseGetChildren2(pureRep)
        Future.value(pureRep.body)
    }
  }


  def getData(path: String, watcher: Boolean): Future[Option[GetDataResponseBody]] = {
    require(path.length != 0, "Path must be longer than 0")
    require(watcher || !watcher, "Watch must be true or false")

    client.getData(path, watcher, connectionManager.getXid) flatMap {
      rep =>
        val pureRep = rep.asInstanceOf[GetDataResponse]
        parseGetData(pureRep.asInstanceOf[GetDataResponse])
        Future.value(pureRep.asInstanceOf[GetDataResponse].body)
    }
  }

  def sendPing: Future[Option[ReplyHeader]] = {
    client.ping flatMap { rep =>
      Try {rep.asInstanceOf[ReplyHeader]} match {
        case Return(res) =>
          connectionManager.parseReplyHeader(res)
          Future.value(Some(res))

        case Throw(ex) =>
          logger.warning(ex.getMessage + ": " + ex.getCause)
          Future.value(None)
      }
    }
  }

  def setACL(path: String, acl: Array[ACL], version: Int): Future[Option[SetACLResponseBody]] = {
    require(path.length != 0, "Path must be longer than 0")
    require(acl.size != 0, "ACL list must not be empty")

    client.setACL(path, acl, version, connectionManager.getXid) flatMap {
      rep =>
        val pureRep = rep.asInstanceOf[SetACLResponse]
        parseSetAcl(pureRep)
        Future.value(pureRep.body)
    }
  }

  def setData(path: String, data: Array[Byte], version: Int): Future[Option[SetDataResponseBody]] = {
    require(path.length != 0, "Path must be longer than 0")

    client.setData(path, data, version, connectionManager.getXid) flatMap {
      rep =>
        val pureRep = rep.asInstanceOf[SetDataResponse]
        parseSetData(pureRep)
        Future.value(pureRep.body)
    }
  }

  def setWatches(relativeZxid: Int,
    dataWatches: Array[String],
    existsWatches: Array[String],
    childWatches: Array[String]): Future[Option[ReplyHeader]] = {

    client.setWatches(relativeZxid, dataWatches, existsWatches, childWatches, connectionManager.getXid) flatMap {
      rep =>
        val pureRep = rep.asInstanceOf[ReplyHeader]
        parseSetWatches(pureRep)
        Future.value(Some(pureRep))
    }
  }

  def sync(path: String): Future[Option[SyncResponseBody]] = {
    require(path.length != 0, "Path must be longer than 0")

    client.sync(path, connectionManager.getXid) flatMap {
      rep =>
        val pureRep = rep.asInstanceOf[SyncResponse]
        parseSync(pureRep)
        Future.value(pureRep.body)
    }
  }

  def transaction(opList: Array[OpRequest]): Future[Option[Array[OpResult]]] = {
    client.transaction(opList, connectionManager.getXid) flatMap {
      rep =>
        val pureRep = rep.asInstanceOf[TransactionResponse]
        Future.value(Some(pureRep.responseList))

    }
  }

  def parseCreate(rep: CreateResponse) = {
    connectionManager.parseReplyHeader(rep.header)
    println("--->Create response | path: " + rep.header.err)
  }

  def parseDelete(rep: ReplyHeader) = {
    connectionManager.parseReplyHeader(rep)
    println("--->Delete response | err: " + rep.err)
  }

  def parseExists(rep: ExistsResponse) = {
    connectionManager.parseReplyHeader(rep.header)
    if (rep.header.err == 0)
      println("--->Exists Response | stat: " + rep.body.get.stat)
    else
      println("--->Exists Response | No node")
  }

  def parseGetACL(rep: GetACLResponse) = {
    connectionManager.parseReplyHeader(rep.header)
    println("--->getAcl response " +
      rep.body.get.acl(0).id.scheme +
      " " + rep.body.get.acl(0).id.id +
      " " + rep.body.get.acl(0).perms)
  }

  def parseGetData(rep: GetDataResponse) = {
    connectionManager.parseReplyHeader(rep.header)
    println("--->getData response | err: " + rep.header.err)
  }

  def parseSetData(rep: SetDataResponse) = {
    connectionManager.parseReplyHeader(rep.header)
    if (rep.header.err == 0)
      println("--->setData response | err: " + rep.header.err)
    else
      println("--->setData response | error: " + rep.header.err)
  }

  def parseGetChildren(rep: GetChildrenResponse) = {
    connectionManager.parseReplyHeader(rep.header)
    println("--->getChildren response | err :" + rep.header.err)
  }

  def parseGetChildren2(rep: GetChildren2Response) = {
    connectionManager.parseReplyHeader(rep.header)
    println("--->getChildren2 response | err :" + rep.header.err)
  }

  def parseSetWatches(rep: ReplyHeader) = {
    connectionManager.parseReplyHeader(rep)
    println("--->setWatches response | err: " + rep.err)
  }

  def parseSetAcl(rep: SetACLResponse) = {
    connectionManager.parseReplyHeader(rep.header)
    println("--->setACL response | err: " + rep.header.err)
  }

  def parseSync(rep: SyncResponse) = {
    connectionManager.parseReplyHeader(rep.header)
    println("--->syncResponse | path: " + rep.header.err)
  }

  def parseWatcherEvent(rep: WatcherEvent) = {
    connectionManager.parseReplyHeader(rep.header)
    println("--->watcherEvent ")
  }
}

class ConnexionManager {
  // TODO manage connection state

  var sessionId: Long = 0
  var timeOut: Int = 0
  var passwd: Array[Byte] = Array[Byte](16)
  var lastZxid: Long = 0L
  private[this] var xid: Int = 2
  var connectionState: states.ConnectionState = states.CLOSED

  object states extends Enumeration {
    type ConnectionState = Value
    val CONNECTING, ASSOCIATING, CONNECTED, CONNECTEDREADONLY, CLOSED, AUTH_FAILED, NOT_CONNECTED = Value
  }

  def getXid: Int = {
    this.synchronized {
      xid += 1
      xid - 1
    }
  }

  def parseConnectResponse(rep: ConnectResponse) = {
    println("-->Connection response | timeout: " +
      rep.timeOut + " | sessionID: " +
      rep.sessionId + " | canRO: " +
      rep.canRO.getOrElse(false))

    sessionId = rep.sessionId
    timeOut = rep.timeOut
    passwd = rep.passwd
    connectionState = states.CONNECTED
  }

  def parseReplyHeader(rep: ReplyHeader) = {

    println("-->Header reply | XID: " + rep.xid + " | ZXID: " + rep.zxid + " | ERR: " + rep.err)
    lastZxid = rep.zxid
    if (rep.err != 0) {
      connectionState = states.NOT_CONNECTED

    }
  }

  def realTimeout: Long = timeOut * 2 / 3

}

