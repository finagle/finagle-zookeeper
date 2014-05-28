package com.twitter.finagle.exp.zookeeper.client

import com.twitter.finagle.exp.zookeeper._
import com.twitter.conversions.time._
import com.twitter.util.{Await, Future}


class SessionManager {
  var sessionId: Long = 0
  var timeOut: Int = 0
  var passwd: Array[Byte] = Array[Byte](16)
  var lastZxid: Long = 0L
  @volatile private[this] var xid: Int = 2
  var connectionState: states.ConnectionState = states.CLOSED
  val pingTimer = new PingTimer

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

  private[this] def startPing(f: => Unit) = pingTimer(realTimeout.milliseconds)(f)
  private[this] def stopPing: Unit = pingTimer.stopTimer
  private[this] def realTimeout: Long = timeOut * 2 / 3

  def startSession(response: ConnectResponse, writer: Request => Future[Response]): Unit = {
    parseConnectResponse(response)
    startPing(writer(new PingRequest))
  }

  def stopSession: Unit = {
    stopPing
  }


  def parseConnectResponse(rep: ConnectResponse) = {
    sessionId = rep.sessionId
    timeOut = rep.timeOut
    passwd = rep.passwd
    connectionState = states.CONNECTED
  }

  def parseReplyHeader(rep: ReplyHeader) = {
    println("--->Header reply | XID: " + rep.xid + " | ZXID: " + rep.zxid + " | ERR: " + rep.err)
    lastZxid = rep.zxid
    if (rep.err != 0) {
      connectionState = states.NOT_CONNECTED
    }
  }

  /*def parseCreate(rep: CreateResponse) = {
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
  }*/


}
