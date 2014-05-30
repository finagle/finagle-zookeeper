package com.twitter.finagle.exp.zookeeper

import com.twitter.util.{Future, Try}
import com.twitter.finagle.exp.zookeeper.transport.BufferReader
import com.twitter.finagle.exp.zookeeper.watcher.{zkState, eventType}

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

sealed abstract class Response
sealed trait Decoder[T <: Response] extends (BufferReader => Try[T]) {
  def apply(br: BufferReader): Try[T] = Try(decode(br))
  def decode(br: BufferReader): T
}

case class ConnectResponse(
  protocolVersion: Int,
  timeOut: Int,
  sessionId: Long,
  passwd: Array[Byte],
  canRO: Option[Boolean]
  ) extends Response

case class CreateResponse(path: String) extends Response
case class ExistsResponse(stat: Stat, watch: Option[Future[WatcherEvent]]) extends Response
case class ErrorResponse(err: Int) extends Response
class EmptyResponse extends Response
case class GetACLResponse(acl: Array[ACL], stat: Stat) extends Response
case class GetChildrenResponse(
  children: Array[String],
  watch: Option[Future[WatcherEvent]])
  extends Response

case class GetChildren2Response(
  children: Array[String],
  stat: Stat,
  watch: Option[Future[WatcherEvent]])
  extends Response

case class GetDataResponse(
  data: Array[Byte],
  stat: Stat,
  watch: Option[Future[WatcherEvent]])
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
  responseList: Array[OpResult])
  extends Response

case class WatcherEvent(typ: Int, state: Int, path: String) extends Response

object ConnectResponse extends Decoder[ConnectResponse] {
  override def decode(br: BufferReader): ConnectResponse = {

    val protocolVersion = br.readInt
    val timeOut = br.readInt
    val sessionId = br.readLong
    val passwd: Array[Byte] = br.readBuffer
    val canRO: Option[Boolean] = {
      try {
        Some(br.readBool)
      } catch {
        case ex: Exception => throw ex
      }
    }

    new ConnectResponse(protocolVersion,
      timeOut,
      sessionId,
      passwd,
      canRO)
  }
}

object CreateResponse extends Decoder[CreateResponse] {
  override def decode(br: BufferReader): CreateResponse = {

    new CreateResponse(br.readString)
  }
}

object ErrorResponse extends Decoder[ErrorResponse] {
  override def decode(br: BufferReader): ErrorResponse = {
    // TODO Use this

    new ErrorResponse(br.readInt)
  }
}

object ExistsResponse {
  def decodeWithWatch(br: BufferReader, watch: Option[Future[WatcherEvent]]): Try[ExistsResponse] = Try {

    new ExistsResponse(Stat.decode(br), watch)
  }
}

object GetACLResponse extends Decoder[GetACLResponse] {
  override def decode(br: BufferReader): GetACLResponse = {

    val aclList = ACL.decodeArray(br)
    val stat = Stat.decode(br)
    new GetACLResponse(aclList, stat)
  }
}

object GetChildrenResponse {
  def decodeWithWatch(br: BufferReader, watch: Option[Future[WatcherEvent]]): Try[GetChildrenResponse] = Try {

    val size = br.readInt
    val children = new Array[String](size)

    for (i <- 0 to size - 1) {
      children(i) = br.readString
    }

    new GetChildrenResponse(children, watch)
  }
}

object GetChildren2Response {
  def decodeWithWatch(br: BufferReader, watch: Option[Future[WatcherEvent]]): Try[GetChildren2Response] = Try {
    val size = br.readInt
    val children = new Array[String](size)

    for (i <- 0 to size - 1) {
      children(i) = br.readString
    }
    new GetChildren2Response(children, Stat.decode(br), watch)
  }
}

object GetDataResponse {
  def decodeWithWatch(br: BufferReader, watch: Option[Future[WatcherEvent]]): Try[GetDataResponse] = Try {

    val data = br.readBuffer
    val stat = Stat.decode(br)

    new GetDataResponse(data, stat, watch)
  }
}

object GetMaxChildrenResponse extends Decoder[GetMaxChildrenResponse] {
  override def decode(br: BufferReader): GetMaxChildrenResponse = {

    new GetMaxChildrenResponse(br.readInt)
  }
}

object ReplyHeader extends Decoder[ReplyHeader] {
  override def decode(br: BufferReader): ReplyHeader = {

    val xid = br.readInt
    val zxid = br.readLong
    val err = br.readInt

    if (err == 0)
      new ReplyHeader(xid, zxid, err)
    else {
      throw ZookeeperException.create("Error :", err)
    }
  }
}

object SetACLResponse extends Decoder[SetACLResponse] {
  override def decode(br: BufferReader): SetACLResponse = {

    new SetACLResponse(Stat.decode(br))
  }
}

object SetDataResponse extends Decoder[SetDataResponse] {
  override def decode(br: BufferReader): SetDataResponse = {

    new SetDataResponse(Stat.decode(br))
  }
}

object SyncResponse extends Decoder[SyncResponse] {
  override def decode(br: BufferReader): SyncResponse = {

    new SyncResponse(br.readString)
  }
}

object TransactionResponse extends Decoder[TransactionResponse] {
  override def decode(br: BufferReader): TransactionResponse = {

    new TransactionResponse(Transaction.decode(br))
  }
}

object WatcherEvent extends Decoder[WatcherEvent] {
  override def decode(br: BufferReader): WatcherEvent = {

    val typ = br.readInt
    val state = br.readInt
    val path = br.readString

    println("---[ Event type: %s | state: %s | path: %s  ]".format(eventType.getEvent(typ), zkState.getState(state), path))

    new WatcherEvent(typ, state, path)
  }
}