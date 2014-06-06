package com.twitter.finagle.exp.zookeeper

import com.twitter.finagle.exp.zookeeper.transport._
import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.OpCode
import com.twitter.finagle.exp.zookeeper.data.ACL
import com.twitter.io.Buf
import com.twitter.finagle.exp.zookeeper.data.Auth
import scala.Some

/**
 * Same as the Response type, a Request can be composed by a header or
 * by body + header.
 *
 * However ConnectRequest is an exception, it is only composed of a body.
 */

sealed trait Request {
  def buf: Buf
}

case class RequestHeader(xid: Int, opCode: Int) {
  def buf: Buf = Buf.Empty
    .concat(BufInt(xid))
    .concat(BufInt(opCode))
}

case class AuthRequest(
  typ: Int,
  auth: Auth
  ) extends Request {
  def buf: Buf = Buf.Empty
    .concat(BufInt(typ))
    .concat(BufString(auth.scheme))
    .concat(BufArray(auth.data))
}

case class CheckWatchesRequest(
  path: String,
  typ: Int
  ) extends Request {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufInt(typ))
}

case class ConnectRequest(
  protocolVersion: Int = 0,
  lastZxidSeen: Long = 0L,
  connectionTimeout: Int = 2000,
  sessionId: Long = 0L,
  passwd: Array[Byte] = Array[Byte](16),
  canBeRO: Option[Boolean] = Some(true))
  extends Request {
  def buf: Buf = Buf.Empty
    .concat(BufInt(protocolVersion))
    .concat(BufLong(lastZxidSeen))
    .concat(BufInt(connectionTimeout))
    .concat(BufLong(sessionId))
    .concat(BufArray(passwd))
    .concat(BufBool(canBeRO.getOrElse(false)))
}

case class CreateRequest(
  path: String,
  data: Array[Byte],
  aclList: Array[ACL],
  createMode: Int)
  extends Request {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufArray(data))
    .concat(BufSeqACL(aclList))
    .concat(BufInt(createMode))
}


case class GetACLRequest(path: String)
  extends Request {
  def buf: Buf = BufString(path)
}

case class GetDataRequest(path: String, watch: Boolean)
  extends Request {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufBool(watch))
}

case class GetMaxChildrenRequest(path: String)
  extends Request {
  def buf: Buf = BufString(path)
}

case class DeleteRequest(path: String, version: Int)
  extends Request {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufInt(version))
}

case class ExistsRequest(path: String, watch: Boolean)
  extends Request {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufBool(watch))
}

class PingRequest extends RequestHeader(-2, OpCode.PING) with Request
class CloseSessionRequest extends RequestHeader(1, OpCode.CLOSE_SESSION) with Request
case class SetDataRequest(
  path: String,
  data: Array[Byte],
  version: Int)
  extends Request {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufArray(data))
    .concat(BufInt(version))
}

case class GetChildrenRequest(path: String, watch: Boolean)
  extends Request {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufBool(watch))
}

case class GetChildren2Request(path: String, watch: Boolean)
  extends Request {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufBool(watch))
}

case class ReconfigRequest(
  joiningServers: String,
  leavingServers: String,
  newMembers: String,
  curConfigId: Long
  ) extends Request {
  def buf: Buf = Buf.Empty
    .concat(BufString(joiningServers))
    .concat(BufString(leavingServers))
    .concat(BufString(newMembers))
    .concat(BufLong(curConfigId))
}

case class RemoveWatchesRequest(
  path: String,
  typ: Int
  ) extends Request {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufInt(typ))
}

case class SetACLRequest(path: String, acl: Array[ACL], version: Int)
  extends Request {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufSeqACL(acl))
    .concat(BufInt(version))
}

case class SetMaxChildrenRequest(path: String, max: Int)
  extends Request {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufInt(max))
}

case class SyncRequest(path: String)
  extends Request {
  def buf: Buf = BufString(path)
}

/* Only available during client reconnection to set watches */
case class SetWatchesRequest(
  relativeZxid: Int,
  dataWatches: Array[String],
  existWatches: Array[String],
  childWatches: Array[String])
  extends Request {
  def buf: Buf = Buf.Empty
    .concat(BufLong(relativeZxid))
    .concat(BufSeqString(dataWatches))
    .concat(BufSeqString(existWatches))
    .concat(BufSeqString(childWatches))
}

case class TransactionRequest(transaction: Transaction)
  extends Request {
  def buf: Buf = transaction.buf
}