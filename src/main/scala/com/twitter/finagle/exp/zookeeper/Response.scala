package com.twitter.finagle.exp.zookeeper

import com.twitter.finagle.exp.zookeeper.transport._
import com.twitter.finagle.exp.zookeeper.data.{ACL, Stat}
import com.twitter.io.Buf
import com.twitter.util._
import com.twitter.util.TimeConversions._

sealed trait Response
sealed trait Decoder[T <: Response] {
  def unapply(buffer: Buf): Option[(T, Buf)]
  def apply(buffer: Buf): Try[(T, Buf)] = {
    unapply(buffer) match {
      case Some((rep, rem)) => Return((rep, rem))
      case None => Throw(ZkDecodingException("Error while decoding"))
    }
  }
}

case class ConnectResponse(
  protocolVersion: Int,
  timeOut: Duration,
  sessionId: Long,
  passwd: Array[Byte],
  isRO: Boolean
  ) extends Response

case class CreateResponse(path: String) extends Response

sealed trait ExistsResponse extends Response
case class NodeWithWatch(stat: Stat, watch: Option[Future[WatchEvent]]) extends ExistsResponse
case class NoNodeWatch(watch: Future[WatchEvent]) extends ExistsResponse

case class ErrorResponse(err: Int) extends Response
case class EmptyResponse() extends Response
case class GetACLResponse(acl: Seq[ACL], stat: Stat) extends Response
case class GetChildrenResponse(
  children: Seq[String],
  watch: Option[Future[WatchEvent]])
  extends Response

case class GetChildren2Response(
  children: Seq[String],
  stat: Stat,
  watch: Option[Future[WatchEvent]])
  extends Response

case class GetDataResponse(
  data: Array[Byte],
  stat: Stat,
  watch: Option[Future[WatchEvent]])
  extends Response

case class GetMaxChildrenResponse(max: Int) extends Response
case class SetACLResponse(stat: Stat) extends Response
case class SetDataResponse(stat: Stat) extends Response
case class SyncResponse(path: String) extends Response

case class ReplyHeader(
  xid: Int,
  zxid: Long,
  err: Int)
  extends Response

case class TransactionResponse(
  responseList: Seq[OpResult])
  extends Response

case class WatchEvent(typ: Int, state: Int, path: String) extends Response

/**
 * Decoders
 */

object ConnectResponse extends Decoder[ConnectResponse] {
  def unapply(buf: Buf): Option[(ConnectResponse, Buf)] = {
    val BufInt(protocolVersion,
    BufInt(timeOut,
    BufLong(sessionId,
    BufArray(passwd,
    BufBool(isRO,
    rem
    ))))) = buf
    Some(ConnectResponse(protocolVersion, timeOut.milliseconds, sessionId, passwd, Option(isRO).getOrElse(false)), rem)
  }
}

object CreateResponse extends Decoder[CreateResponse] {
  def unapply(buf: Buf): Option[(CreateResponse, Buf)] = {
    val BufString(path, rem) = buf
    Some(CreateResponse(path), rem)
  }
}

object ErrorResponse extends Decoder[ErrorResponse] {
  def unapply(buf: Buf): Option[(ErrorResponse, Buf)] = {
    val BufInt(err, rem) = buf
    Some(ErrorResponse(err), rem)
  }
}

object ExistsResponse extends Decoder[ExistsResponse] {
  def unapply(buf: Buf): Option[(ExistsResponse, Buf)] = {
    val Stat(stat, rem) = buf
    Some(NodeWithWatch(stat, None), rem)
  }
}

object GetACLResponse extends Decoder[GetACLResponse] {
  def unapply(buf: Buf): Option[(GetACLResponse, Buf)] = {
    val BufSeqACL(acl, Stat(stat, _)) = buf
    Some(GetACLResponse(acl, stat), buf)
  }
}

object GetChildrenResponse extends Decoder[GetChildrenResponse] {
  def unapply(buf: Buf): Option[(GetChildrenResponse, Buf)] = {
    val BufSeqString(children, _) = buf
    Some(GetChildrenResponse(children, None), buf)
  }
}

object GetChildren2Response extends Decoder[GetChildren2Response] {
  def unapply(buf: Buf): Option[(GetChildren2Response, Buf)] = {
    val BufSeqString(children, Stat(stat, _)) = buf
    Some(GetChildren2Response(children, stat, None), buf)
  }
}

object GetDataResponse extends Decoder[GetDataResponse] {
  def unapply(buf: Buf): Option[(GetDataResponse, Buf)] = {
    val BufArray(data, Stat(stat, rem)) = buf
    Some(GetDataResponse(data, stat, None), rem)
  }
}

object GetMaxChildrenResponse extends Decoder[GetMaxChildrenResponse] {
  def unapply(buf: Buf): Option[(GetMaxChildrenResponse, Buf)] = {
    val BufInt(max, rem) = buf
    Some(GetMaxChildrenResponse(max), rem)
  }
}

object ReplyHeader extends Decoder[ReplyHeader] {
  def unapply(buf: Buf): Option[(ReplyHeader, Buf)] = {
    val BufInt(xid, BufLong(zxid, BufInt(err, rem))) = buf
    Some(ReplyHeader(xid, zxid, err), rem)
  }
}

object SetACLResponse extends Decoder[SetACLResponse] {
  def unapply(buf: Buf): Option[(SetACLResponse, Buf)] = {
    val Stat(stat, rem) = buf
    Some(SetACLResponse(stat), rem)
  }
}

object SetDataResponse extends Decoder[SetDataResponse] {
  def unapply(buf: Buf): Option[(SetDataResponse, Buf)] = {
    val Stat(stat, rem) = buf
    Some(SetDataResponse(stat), rem)
  }
}

object SyncResponse extends Decoder[SyncResponse] {
  def unapply(buf: Buf): Option[(SyncResponse, Buf)] = {
    val BufString(path, rem) = buf
    Some(SyncResponse(path), rem)
  }
}

object TransactionResponse extends Decoder[TransactionResponse] {
  def unapply(buf: Buf): Option[(TransactionResponse, Buf)] = {

    // Fixme need to give a partial response in case of failure
    val Transaction(trans, rem) = buf
    (trans, rem) match {
      case res: (TransactionResponse, Buf) => Some(res)
      case _ => None
    }
  }
}

object WatchEvent extends Decoder[WatchEvent] {
  def unapply(buf: Buf): Option[(WatchEvent, Buf)] = {
    val BufInt(typ, BufInt(state, BufString(path, rem))) = buf
    Some(new WatchEvent(typ, state, path), rem)
  }
}