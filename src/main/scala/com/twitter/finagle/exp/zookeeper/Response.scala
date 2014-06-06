package com.twitter.finagle.exp.zookeeper

import com.twitter.util.{Future, Try}
import com.twitter.finagle.exp.zookeeper.transport._
import com.twitter.finagle.exp.zookeeper.data.{ACL, Stat}
import com.twitter.io.Buf

/**
 * This File describes every responses
 * A response is usually composed by a header + a body
 * but there are some exceptions where there is only a header (ReplyHeader).
 * For example a CreateResponse is composed by a ReplyHeader and a CreateResponseBody
 * this way : new CreateResponse(h: ReplyHeader, b: CreateResponseBody).
 *
 * That's the reason why header extends Response.
 *
 * A BufferedResponse is a raw container (BufferReader) of response, as we have many
 * different responses, we use a common wrapper, that we can then decode with
 * the request opCode. With a BufferedResponse and an opCode we can give a Response
 *
 **/

sealed trait Response
sealed trait Decoder[T <: Response] {
  def unapply(buffer: Buf): Option[(T, Buf)]
  def apply(buffer: Buf): Try[(T, Buf)] = Try {
    unapply(buffer) match {
      case Some((rep, rem)) => (rep, rem)
      case None => throw ZkDecodingException("Error while decoding")
    }
  }
}

case class ConnectResponse(
  protocolVersion: Int,
  timeOut: Int,
  sessionId: Long,
  passwd: Array[Byte],
  canRO: Option[Boolean]
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

object ConnectResponse extends Decoder[ConnectResponse] {
  def unapply(buf: Buf): Option[(ConnectResponse, Buf)] = {
    val BufInt(protocolVersion,
    BufInt(timeOut,
    BufLong(sessionId,
    BufArray(passwd,
    rem
    )))) = buf
    Some(ConnectResponse(protocolVersion, timeOut, sessionId, passwd, Some(false)), rem)
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
    Some(new NodeWithWatch(stat, None), rem)
  }
}

object GetACLResponse extends Decoder[GetACLResponse] {
  def unapply(buf: Buf): Option[(GetACLResponse, Buf)] = {
    val BufSeqACL(acl, Stat(stat, rem)) = buf
    Some(new GetACLResponse(acl, stat), buf)
  }
}

object GetChildrenResponse extends Decoder[GetChildrenResponse] {
  def unapply(buf: Buf): Option[(GetChildrenResponse, Buf)] = {
    val BufSeqString(children, rem) = buf
    Some(new GetChildrenResponse(children, None), buf)
  }
}

object GetChildren2Response extends Decoder[GetChildren2Response] {
  def unapply(buf: Buf): Option[(GetChildren2Response, Buf)] = {
    val BufSeqString(children, Stat(stat, rem)) = buf
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
    Some(new GetMaxChildrenResponse(max), rem)
  }
}

object ReplyHeader extends Decoder[ReplyHeader] {
  def unapply(buf: Buf): Option[(ReplyHeader, Buf)] = {
    val BufInt(xid, BufLong(zxid, BufInt(err, rem))) = buf
    Some(new ReplyHeader(xid, zxid, err), rem)
  }
}

object SetACLResponse extends Decoder[SetACLResponse] {
  def unapply(buf: Buf): Option[(SetACLResponse, Buf)] = {
    val Stat(stat, rem) = buf
    Some(new SetACLResponse(stat), rem)
  }
}

object SetDataResponse extends Decoder[SetDataResponse] {
  def unapply(buf: Buf): Option[(SetDataResponse, Buf)] = {
    val Stat(stat, rem) = buf
    Some(new SetDataResponse(stat), rem)
  }
}

object SyncResponse extends Decoder[SyncResponse] {
  def unapply(buf: Buf): Option[(SyncResponse, Buf)] = {
    val BufString(path, rem) = buf
    Some(new SyncResponse(path), rem)
  }
}

object TransactionResponse extends Decoder[TransactionResponse] {
  def unapply(buf: Buf): Option[(TransactionResponse, Buf)] = {

    // Fixme need to give a partial response
    val Transaction(trans, rem) = buf
    Some(trans, rem)
  }
}

object WatchEvent extends Decoder[WatchEvent] {
  def unapply(buf: Buf): Option[(WatchEvent, Buf)] = {
    val BufInt(typ, BufInt(state, BufString(path, rem))) = buf
    Some(new WatchEvent(typ, state, path), rem)
  }
}