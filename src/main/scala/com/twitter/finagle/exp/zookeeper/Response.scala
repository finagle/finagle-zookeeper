package com.twitter.finagle.exp.zookeeper

import com.twitter.finagle.exp.zookeeper.data.{ACL, Stat}
import com.twitter.finagle.exp.zookeeper.transport._
import com.twitter.finagle.exp.zookeeper.watcher.Watcher
import com.twitter.io.Buf
import com.twitter.util._
import com.twitter.util.TimeConversions._

/**
 * Mother of all responses, used to describe what a response can do.
 */
sealed trait GlobalRep
/**
 * Unique response, used to answer to a Request
 */
sealed trait Response extends GlobalRep
/**
 * OpResult is used to compose a Transaction response.
 * A Transaction response can be composed by OpResult only.
 */
sealed trait OpResult extends GlobalRep
/**
 * Special case of Response, a Response header.
 */
private[finagle] trait RepHeader extends GlobalRep
/**
 * Describes what a response decoder must do
 *
 * @tparam T Response type
 */
sealed trait GlobalRepDecoder[T <: GlobalRep] {
  def unapply(buffer: Buf): Option[(T, Buf)]
  def apply(buffer: Buf): Try[(T, Buf)] = unapply(buffer) match {
    case Some((rep, rem)) => Return((rep, rem))
    case None => Throw(ZkDecodingException("Error while decoding"))
  }
}

case class ConnectResponse(
  protocolVersion: Int,
  timeOut: Duration,
  sessionId: Long,
  passwd: Array[Byte],
  isRO: Boolean
  ) extends Response

case class CreateResponse(path: String) extends Response with OpResult

case class Create2Response(
  path: String,
  stat: Stat) extends Response with OpResult

case class ExistsResponse(
  stat: Option[Stat],
  watcher: Option[Watcher]
  ) extends Response

case class ErrorResponse(exception: ZookeeperException) extends OpResult

case class EmptyResponse() extends Response with OpResult

case class GetACLResponse(acl: Seq[ACL], stat: Stat) extends Response

case class GetChildrenResponse(
  children: Seq[String],
  watcher: Option[Watcher]
  ) extends Response

case class GetChildren2Response(
  children: Seq[String],
  stat: Stat,
  watcher: Option[Watcher]
  ) extends Response

case class GetDataResponse(
  data: Array[Byte],
  stat: Stat,
  watcher: Option[Watcher]
  ) extends Response

case class SetACLResponse(stat: Stat) extends Response

case class SetDataResponse(stat: Stat) extends Response with OpResult

case class SyncResponse(path: String) extends Response

case class ReplyHeader(xid: Int, zxid: Long, err: Int) extends RepHeader

case class TransactionResponse(responseList: Seq[OpResult]) extends Response

case class WatchEvent(typ: Int, state: Int, path: String) extends Response


/**
 * Decoders
 */
private[finagle]
object ConnectResponse extends GlobalRepDecoder[ConnectResponse] {
  def unapply(buf: Buf): Option[(ConnectResponse, Buf)] = {
    val Buf.U32BE(protocolVersion,
    Buf.U32BE(timeOut,
    Buf.U64BE(sessionId,
    BufArray(passwd,
    BufBool(isRO,
    rem
    ))))) = buf

    Some(
      ConnectResponse(
        protocolVersion,
        timeOut.milliseconds,
        sessionId,
        passwd,
        Option(isRO).getOrElse(false)),
      rem)
  }
}

private[finagle]
object CreateResponse extends GlobalRepDecoder[CreateResponse] {
  def unapply(buf: Buf): Option[(CreateResponse, Buf)] = {
    val BufString(path, rem) = buf
    Some(CreateResponse(path), rem)
  }
}

private[finagle]
object Create2Response extends GlobalRepDecoder[Create2Response] {
  def unapply(buf: Buf): Option[(Create2Response, Buf)] = {
    val BufString(path, Stat(stat, rem)) = buf
    Some(Create2Response(path, stat), rem)
  }
}

private[finagle]
object ErrorResponse extends GlobalRepDecoder[ErrorResponse] {
  def unapply(buf: Buf): Option[(ErrorResponse, Buf)] = {
    val Buf.U32BE(err, rem) = buf
    Some(ErrorResponse(
      ZookeeperException.create("Exception during the transaction:", err)),
      rem)
  }
}

private[finagle]
object ExistsResponse extends GlobalRepDecoder[ExistsResponse] {
  def unapply(buf: Buf): Option[(ExistsResponse, Buf)] = {
    val Stat(stat, rem) = buf
    Some(ExistsResponse(Some(stat), None), rem)
  }
}

private[finagle]
object GetACLResponse extends GlobalRepDecoder[GetACLResponse] {
  def unapply(buf: Buf): Option[(GetACLResponse, Buf)] = {
    val BufSeqACL(acl, Stat(stat, _)) = buf
    Some(GetACLResponse(acl, stat), buf)
  }
}

private[finagle]
object GetChildrenResponse extends GlobalRepDecoder[GetChildrenResponse] {
  def unapply(buf: Buf): Option[(GetChildrenResponse, Buf)] = {
    val BufSeqString(children, _) = buf
    Some(GetChildrenResponse(children, None), buf)
  }
}

private[finagle]
object GetChildren2Response extends GlobalRepDecoder[GetChildren2Response] {
  def unapply(buf: Buf): Option[(GetChildren2Response, Buf)] = {
    val BufSeqString(children, Stat(stat, _)) = buf
    Some(GetChildren2Response(children, stat, None), buf)
  }
}

private[finagle]
object GetDataResponse extends GlobalRepDecoder[GetDataResponse] {
  def unapply(buf: Buf): Option[(GetDataResponse, Buf)] = {
    val BufArray(data, Stat(stat, rem)) = buf
    Some(GetDataResponse(data, stat, None), rem)
  }
}

private[finagle]
object ReplyHeader extends GlobalRepDecoder[ReplyHeader] {
  def unapply(buf: Buf): Option[(ReplyHeader, Buf)] = {
    val Buf.U32BE(xid, Buf.U64BE(zxid, Buf.U32BE(err, rem))) = buf
    Some(ReplyHeader(xid, zxid, err), rem)
  }
}

private[finagle]
object SetACLResponse extends GlobalRepDecoder[SetACLResponse] {
  def unapply(buf: Buf): Option[(SetACLResponse, Buf)] = {
    val Stat(stat, rem) = buf
    Some(SetACLResponse(stat), rem)
  }
}

private[finagle]
object SetDataResponse extends GlobalRepDecoder[SetDataResponse] {
  def unapply(buf: Buf): Option[(SetDataResponse, Buf)] = {
    val Stat(stat, rem) = buf
    Some(SetDataResponse(stat), rem)
  }
}

private[finagle]
object SyncResponse extends GlobalRepDecoder[SyncResponse] {
  def unapply(buf: Buf): Option[(SyncResponse, Buf)] = {
    val BufString(path, rem) = buf
    Some(SyncResponse(path), rem)
  }
}

private[finagle]
object TransactionResponse extends GlobalRepDecoder[TransactionResponse] {
  def unapply(buf: Buf): Option[(TransactionResponse, Buf)] = {
    Transaction.decode(Seq.empty[OpResult], buf) match {
      case (opList: Seq[OpResult], buf: Buf) =>
        Some((TransactionResponse(opList), buf))
      case _ => None
    }
  }
}

private[finagle]
object WatchEvent extends GlobalRepDecoder[WatchEvent] {
  def unapply(buf: Buf): Option[(WatchEvent, Buf)] = {
    val Buf.U32BE(typ, Buf.U32BE(state, BufString(path, rem))) = buf
    Some(new WatchEvent(typ, state, path), rem)
  }
}