package com.twitter.finagle.exp.zookeeper

import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.OpCode
import com.twitter.finagle.exp.zookeeper.connection.ConnectionManager
import com.twitter.finagle.exp.zookeeper.data.{ACL, Auth}
import com.twitter.finagle.exp.zookeeper.session.SessionManager
import com.twitter.finagle.exp.zookeeper.transport._
import com.twitter.finagle.exp.zookeeper.watch.WatchManager
import com.twitter.io.Buf
import com.twitter.util.Duration

/**
 * Mother of all requests
 */
sealed trait GlobalRequest {def buf: Buf }
/**
 * OpRequest is used to compose a Transaction request.
 * A Transaction request can be composed by OpRequest only.
 */
sealed trait OpRequest extends GlobalRequest
/**
 * Unique request, used to send basic zookeeper request
 * and also to configure the dispatcher at session creation.
 * opCode describes the operation code of the request
 */
sealed trait Request extends GlobalRequest {
  val opCode: Option[Int]
}
/**
 * Special case of request header, used for RequestHeader
 * It as the same properties as a Request, but can be used
 * as one. (ie ReqPacket composed of Option[ReqHeader] +
 * Option[Request])
 */
private[finagle] trait ReqHeader extends GlobalRequest

private[finagle]
case class AuthRequest(typ: Int = 0, auth: Auth) extends Request {

  override val opCode: Option[Int] = Some(OpCode.AUTH)
  def buf: Buf = Buf.Empty
    .concat(Buf.U32BE(typ))
    .concat(auth.buf)
}

case class CheckVersionRequest(path: String, version: Int) extends OpRequest {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(Buf.U32BE(version))
}

private[finagle]
case class CheckWatchesRequest(path: String, typ: Int) extends Request {

  override val opCode: Option[Int] = Some(OpCode.CHECK_WATCHES)
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(Buf.U32BE(typ))
}

private[finagle] case class ConfigureRequest(
  connectionManager: ConnectionManager,
  sessionManagr: SessionManager,
  watchManagr: WatchManager
  ) extends Request {

  override val opCode: Option[Int] = None
  def buf: Buf = Buf.Empty
}

private[finagle] case class ConnectRequest(
  protocolVersion: Int,
  lastZxidSeen: Long,
  sessionTimeout: Duration,
  sessionId: Long,
  passwd: Array[Byte],
  canBeRO: Boolean
  ) extends Request {

  override val opCode: Option[Int] = Some(OpCode.CREATE_SESSION)
  def buf: Buf = Buf.Empty
    .concat(Buf.U32BE(protocolVersion))
    .concat(Buf.U64BE(lastZxidSeen))
    .concat(Buf.U32BE(sessionTimeout.inMilliseconds.toInt))
    .concat(Buf.U64BE(sessionId))
    .concat(BufArray(passwd))
    .concat(BufBool(canBeRO))
}

case class CreateRequest(
  path: String,
  data: Array[Byte],
  aclList: Seq[ACL],
  createMode: Int
  ) extends Request with OpRequest {

  override val opCode: Option[Int] = Some(OpCode.CREATE)
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufArray(data))
    .concat(BufSeqACL(aclList))
    .concat(Buf.U32BE(createMode))
}

case class Create2Request(
  path: String,
  data: Array[Byte],
  aclList: Seq[ACL],
  createMode: Int
  ) extends Request with OpRequest {

  override val opCode: Option[Int] = Some(OpCode.CREATE2)
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufArray(data))
    .concat(BufSeqACL(aclList))
    .concat(Buf.U32BE(createMode))
}

case class DeleteRequest(path: String, version: Int)
  extends Request with OpRequest {

  override val opCode: Option[Int] = Some(OpCode.DELETE)
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(Buf.U32BE(version))
}

private[finagle]
case class ExistsRequest(path: String, watch: Boolean) extends Request {

  override val opCode: Option[Int] = Some(OpCode.EXISTS)
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufBool(watch))
}

private[finagle] case class GetACLRequest(path: String) extends Request {

  override val opCode: Option[Int] = Some(OpCode.GET_ACL)
  def buf: Buf = BufString(path)
}

private[finagle]
case class GetDataRequest(path: String, watch: Boolean) extends Request {

  override val opCode: Option[Int] = Some(OpCode.GET_DATA)
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufBool(watch))
}

private[finagle]
case class GetChildrenRequest(path: String, watch: Boolean) extends Request {

  override val opCode: Option[Int] = Some(OpCode.GET_CHILDREN)
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufBool(watch))
}

private[finagle]
case class GetChildren2Request(path: String, watch: Boolean) extends Request {

  override val opCode: Option[Int] = Some(OpCode.GET_CHILDREN2)
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufBool(watch))
}

private[finagle] case class ReconfigRequest(
  joiningServers: String,
  leavingServers: String,
  newMembers: String,
  curConfigId: Long
  ) extends Request {

  override val opCode: Option[Int] = Some(OpCode.RECONFIG)
  def buf: Buf = Buf.Empty
    .concat(BufString(joiningServers))
    .concat(BufString(leavingServers))
    .concat(BufString(newMembers))
    .concat(Buf.U64BE(curConfigId))
}

private[finagle] case class RemoveWatchesRequest(
  path: String,
  typ: Int
  ) extends Request {

  override val opCode: Option[Int] = Some(OpCode.REMOVE_WATCHES)
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(Buf.U32BE(typ))
}

private[finagle]
case class RequestHeader(xid: Int, opCode: Int) extends RepHeader {
  def buf: Buf = Buf.Empty
    .concat(Buf.U32BE(xid))
    .concat(Buf.U32BE(opCode))
}

private[finagle]
case class SetACLRequest(path: String, acl: Seq[ACL], version: Int)
  extends Request {

  override val opCode: Option[Int] = Some(OpCode.SET_ACL)
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufSeqACL(acl))
    .concat(Buf.U32BE(version))
}

case class SetDataRequest(
  path: String,
  data: Array[Byte],
  version: Int
  ) extends Request with OpRequest {

  override val opCode: Option[Int] = Some(OpCode.SET_DATA)
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufArray(data))
    .concat(Buf.U32BE(version))
}

/* Only available during client reconnection to set back watches */
private[finagle] case class SetWatchesRequest(
  relativeZxid: Long,
  dataWatches: Seq[String],
  existWatches: Seq[String],
  childWatches: Seq[String]
  ) extends Request {

  override val opCode: Option[Int] = Some(OpCode.SET_WATCHES)
  def buf: Buf = Buf.Empty
    .concat(Buf.U64BE(relativeZxid))
    .concat(BufSeqString(dataWatches))
    .concat(BufSeqString(existWatches))
    .concat(BufSeqString(childWatches))
}

private[finagle] case class SyncRequest(path: String) extends Request {

  override val opCode: Option[Int] = Some(OpCode.SYNC)
  def buf: Buf = BufString(path)
}

private[finagle]
case class TransactionRequest(opList: Seq[OpRequest]) extends Request {

  override val opCode: Option[Int] = Some(OpCode.MULTI)
  def buf: Buf =
    opList.foldLeft(Buf.Empty)((buf, op) =>
      op match {
        case op: CreateRequest =>
          buf.concat(MultiHeader(OpCode.CREATE, false, -1).buf)
            .concat(op.buf)
        case op: Create2Request =>
          buf.concat(MultiHeader(OpCode.CREATE2, false, -1).buf)
            .concat(op.buf)
        case op: DeleteRequest =>
          buf.concat(MultiHeader(OpCode.DELETE, false, -1).buf)
            .concat(op.buf)
        case op: SetDataRequest =>
          buf.concat(MultiHeader(OpCode.SET_DATA, false, -1).buf)
            .concat(op.buf)
        case op: CheckVersionRequest =>
          buf.concat(MultiHeader(OpCode.CHECK, false, -1).buf)
            .concat(op.buf)
        case _ => throw new ZookeeperException("Invalid type of op")
      })
      .concat(MultiHeader(-1, true, -1).buf)
}

private[finagle] object Request {
  def toReqPacket(req: Request, nextXid: => Int): ReqPacket = {
    ReqPacket(
      Some(RequestHeader(nextXid, req.opCode.get)),
      Some(req)
    )
  }
}