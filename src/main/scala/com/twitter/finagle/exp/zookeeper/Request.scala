package com.twitter.finagle.exp.zookeeper

import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.OpCode
import com.twitter.finagle.exp.zookeeper.connection.ConnectionManager
import com.twitter.finagle.exp.zookeeper.data.{ACL, Auth}
import com.twitter.finagle.exp.zookeeper.session.SessionManager
import com.twitter.finagle.exp.zookeeper.transport._
import com.twitter.finagle.exp.zookeeper.watch.WatchManager
import com.twitter.io.Buf
import com.twitter.util.Duration

sealed trait GlobalRequest {def buf: Buf }
private[finagle] trait ReqHeader extends GlobalRequest
sealed trait OpRequest extends GlobalRequest
sealed trait Request extends GlobalRequest {
  val opCode: Option[Int]
}

private[finagle]
case class AuthRequest(typ: Int = 0, auth: Auth) extends Request {

  override val opCode: Option[Int] = Some(OpCode.AUTH)
  def buf: Buf = Buf.Empty
    .concat(BufInt(typ))
    .concat(auth.buf)
}

private[finagle]
case class CheckVersionRequest(path: String, version: Int) extends OpRequest {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufInt(version))
}

private[finagle]
case class CheckWatchesRequest(path: String, typ: Int) extends Request {

  override val opCode: Option[Int] = Some(OpCode.CHECK_WATCHES)
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufInt(typ))
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
    .concat(BufInt(protocolVersion))
    .concat(BufLong(lastZxidSeen))
    .concat(BufInt(sessionTimeout.inMilliseconds.toInt))
    .concat(BufLong(sessionId))
    .concat(BufArray(passwd))
    .concat(BufBool(canBeRO))
}

private[finagle] case class CreateRequest(
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
    .concat(BufInt(createMode))
}

private[finagle] case class Create2Request(
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
    .concat(BufInt(createMode))
}

private[finagle] case class DeleteRequest(path: String, version: Int)
  extends Request with OpRequest {

  override val opCode: Option[Int] = Some(OpCode.DELETE)
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufInt(version))
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
    .concat(BufLong(curConfigId))
}

private[finagle] case class RemoveWatchesRequest(
  path: String,
  typ: Int
  ) extends Request {

  override val opCode: Option[Int] = Some(OpCode.REMOVE_WATCHES)
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufInt(typ))
}

private[finagle]
case class RequestHeader(xid: Int, opCode: Int) extends RepHeader {
  def buf: Buf = Buf.Empty
    .concat(BufInt(xid))
    .concat(BufInt(opCode))
}

private[finagle]
case class SetACLRequest(path: String, acl: Array[ACL], version: Int)
  extends Request {

  override val opCode: Option[Int] = Some(OpCode.SET_ACL)
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufSeqACL(acl))
    .concat(BufInt(version))
}

private[finagle] case class SetDataRequest(
  path: String,
  data: Array[Byte],
  version: Int
  ) extends Request with OpRequest {

  override val opCode: Option[Int] = Some(OpCode.SET_DATA)
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufArray(data))
    .concat(BufInt(version))
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
    .concat(BufLong(relativeZxid))
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