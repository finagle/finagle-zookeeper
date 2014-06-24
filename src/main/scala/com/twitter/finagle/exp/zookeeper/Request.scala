package com.twitter.finagle.exp.zookeeper

import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.OpCode
import com.twitter.finagle.exp.zookeeper.connection.ConnectionManager
import com.twitter.finagle.exp.zookeeper.data.{ACL, Auth}
import com.twitter.finagle.exp.zookeeper.session.SessionManager
import com.twitter.finagle.exp.zookeeper.transport._
import com.twitter.finagle.exp.zookeeper.watch.WatchManager
import com.twitter.io.Buf
import com.twitter.util.Duration
import com.twitter.util.TimeConversions._

/**
 * Same as the Response type, a Request can be composed by a header or
 * by body + header.
 *
 * However ConnectRequest is an exception, it is only composed of a body.
 */

sealed trait Request {
  def buf: Buf
}

sealed trait OpRequest {
  def buf: Buf
}

case class RequestHeader(xid: Int, opCode: Int) {
  def buf: Buf = Buf.Empty
    .concat(BufInt(xid))
    .concat(BufInt(opCode))
}

case class AuthRequest(typ: Int, auth: Auth) extends Request {
  def buf: Buf = Buf.Empty
    .concat(BufInt(typ))
    .concat(BufString(auth.scheme))
    .concat(BufArray(auth.data))
}

case class CheckVersionRequest(path: String, version: Int)
  extends OpRequest {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufInt(version))
}

case class CheckWatchesRequest(path: String, typ: Int) extends Request {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufInt(typ))
}

case class ConfigureRequest(
  connectionManager: Option[ConnectionManager],
  sessionManagr: Option[SessionManager],
  watchManagr: Option[WatchManager]
  ) extends Request {
  def buf: Buf = Buf.Empty
}

case class ConnectRequest(
  protocolVersion: Int = 0,
  lastZxidSeen: Long = 0L,
  sessionTimeout: Duration = 2000.milliseconds,
  sessionId: Long = 0L,
  passwd: Array[Byte] = Array[Byte](16),
  canBeRO: Boolean = true)
  extends Request {
  def buf: Buf = Buf.Empty
    .concat(BufInt(protocolVersion))
    .concat(BufLong(lastZxidSeen))
    .concat(BufInt(sessionTimeout.inMilliseconds.toInt))
    .concat(BufLong(sessionId))
    .concat(BufArray(passwd))
    .concat(BufBool(canBeRO))
}

case class CreateRequest(
  path: String,
  data: Array[Byte],
  aclList: Seq[ACL],
  createMode: Int)
  extends Request with OpRequest {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufArray(data))
    .concat(BufSeqACL(aclList))
    .concat(BufInt(createMode))
}

case class Create2Request(
  path: String,
  data: Array[Byte],
  aclList: Seq[ACL],
  createMode: Int) extends Request with OpRequest {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufArray(data))
    .concat(BufSeqACL(aclList))
    .concat(BufInt(createMode))
}

case class DeleteRequest(path: String, version: Int)
  extends Request with OpRequest {
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

case class SetDataRequest(
  path: String,
  data: Array[Byte],
  version: Int)
  extends Request with OpRequest {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufArray(data))
    .concat(BufInt(version))
}

/*case class GetMaxChildrenRequest(path: String)
  extends Request {
  def buf: Buf = BufString(path)
}
case class SetMaxChildrenRequest(path: String, max: Int)
  extends Request {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufInt(max))
}*/

/* Only available during client reconnection to set watches */
case class SetWatchesRequest(
  relativeZxid: Long,
  dataWatches: Seq[String],
  existWatches: Seq[String],
  childWatches: Seq[String])
  extends Request {
  def buf: Buf = Buf.Empty
    .concat(BufLong(relativeZxid))
    .concat(BufSeqString(dataWatches))
    .concat(BufSeqString(existWatches))
    .concat(BufSeqString(childWatches))
}

case class SyncRequest(path: String)
  extends Request {
  def buf: Buf = BufString(path)
}

case class TransactionRequest(opList: Seq[OpRequest])
  extends Request {
  def buf: Buf =
    opList.foldLeft(Buf.Empty)((buf, op) =>
      op match {
        case op: CreateRequest => buf.concat(MultiHeader(OpCode.CREATE, false, -1).buf)
          .concat(op.buf)
        case op: Create2Request =>
          buf.concat(MultiHeader(OpCode.CREATE2, false, -1).buf)
            .concat(op.buf)
        case op: DeleteRequest => buf.concat(MultiHeader(OpCode.DELETE, false, -1).buf)
          .concat(op.buf)
        case op: SetDataRequest => buf.concat(MultiHeader(OpCode.SET_DATA, false, -1).buf)
          .concat(op.buf)
        case op: CheckVersionRequest => buf.concat(MultiHeader(OpCode.CHECK, false, -1).buf)
          .concat(op.buf)
        case _ => throw new ZookeeperException("Invalid type of op")
      })
      .concat(MultiHeader(-1, true, -1).buf)
}