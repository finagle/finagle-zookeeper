package com.twitter.finagle.exp.zookeeper

import com.twitter.util.Try
import com.twitter.finagle.exp.zookeeper.transport.{Buffer, BufferReader}
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers._
import java.nio.ByteBuffer
import com.twitter.finagle.exp.zookeeper.ZookeeperDefinitions.opCode._
import scala.Some
import scala.collection.mutable.ArrayBuffer
import com.twitter.finagle.exp.zookeeper.ZookeeperDefinitions.opCode

/**
 * This File describes every responses
 * A response is usually composed by a header + a body
 * but there are some exceptions where there is only a header (ReplyHeader).
 * For example a CreateResponse is composed by a ReplyHeader and a CreateResponseBody
 * this way : new CreateResponse(h: ReplyHeader, b: CreateResponseBody).
 *
 * That's the reason why header extends Response.
 *
 * A BufferedResponse is a raw container (buffer) of response, as we have many
 * different responses, we use a common wrapper, that we can then decode with
 * the request opCode. With a BufferedResponse and an opCode we can give a Response
 *
 **/

sealed abstract class Response
sealed trait Header
sealed trait ResponseBody
sealed trait Decoder[T <: Response] extends (Buffer => Try[T]) {
  def apply(buffer: Buffer): Try[T] = Try(decode(buffer))

  def decode(buffer: Buffer): T
}

case class BufferedResponse(buffer: Buffer) extends Response
case class ConnectResponse(
  protocolVersion: Int,
  timeOut: Int,
  sessionId: Long,
  passwd: Array[Byte],
  canRO: Option[Boolean]
  ) extends Response

case class CreateResponseBody(path: String) extends ResponseBody
case class CreateResponse(header: ReplyHeader,
  body: Option[CreateResponseBody]
  ) extends Response

case class ExistsResponseBody(stat: Stat) extends ResponseBody
case class ExistsResponse(header: ReplyHeader,
  body: Option[ExistsResponseBody]
  ) extends Response

case class ErrorResponseBody(err: Int) extends ResponseBody
case class ErrorResponse(header: ReplyHeader,
  body: Option[ErrorResponseBody]
  ) extends Response

case class GetACLResponseBody(acl: Array[ACL], stat: Stat) extends ResponseBody
case class GetACLResponse(header: ReplyHeader,
  body: Option[GetACLResponseBody]
  ) extends Response

case class GetChildrenResponseBody(children: Array[String]) extends ResponseBody
case class GetChildrenResponse(header: ReplyHeader,
  body: Option[GetChildrenResponseBody]) extends Response

case class GetChildren2ResponseBody(children: Array[String],
  stat: Stat) extends ResponseBody
case class GetChildren2Response(header: ReplyHeader,
  body: Option[GetChildren2ResponseBody]) extends Response

case class GetDataResponse(header: ReplyHeader,
  body: Option[GetDataResponseBody]) extends Response
case class GetDataResponseBody(data: Array[Byte], stat: Stat) extends ResponseBody

case class SetACLResponseBody(stat: Stat) extends ResponseBody
case class SetACLResponse(header: ReplyHeader,
  body: Option[SetACLResponseBody]) extends Response

case class SetDataResponseBody(stat: Stat) extends ResponseBody
case class SetDataResponse(header: ReplyHeader,
  body: Option[SetDataResponseBody]) extends Response

case class SyncResponseBody(path: String) extends ResponseBody
case class SyncResponse(header: ReplyHeader,
  body: Option[SyncResponseBody]) extends Response

case class ReplyHeader(xid: Int, zxid: Long,
  err: Int) extends Response
case class TransactionResponse(header: ReplyHeader,
  responseList: Array[OpResult]) extends Response

case class WatcherEventBody(typ: Int, state: Int, path: String) extends ResponseBody
case class WatcherEvent(header: ReplyHeader,
  body: Option[WatcherEventBody]) extends Response

/* To create a BufferedResponse from different objects*/
object BufferedResponse {
  def factory(buffer: Buffer) = new BufferedResponse(buffer)

  def factory(buffer: ChannelBuffer) = new BufferedResponse(Buffer.fromChannelBuffer(buffer))

  def factory(buffer: ByteBuffer) = new BufferedResponse(Buffer.fromChannelBuffer(wrappedBuffer(buffer)))
}

object ResponseDecoder {
  /* Decode a BufferedResponse to type Try[T] by pattern matching opCode (XID) */
  def decode[T >: Response](repBuffer: BufferedResponse, opCode: Int): Try[T] = opCode match {
    case `createSession` => ConnectResponse(repBuffer.buffer)
    case `ping` => ReplyHeader(repBuffer.buffer)
    case `closeSession` => ReplyHeader(repBuffer.buffer)
    case `check` => ReplyHeader(repBuffer.buffer)
    case `create` => CreateResponse(repBuffer.buffer)
    case `delete` => ReplyHeader(repBuffer.buffer)
    case `exists` => ExistsResponse(repBuffer.buffer)
    case `getACL` => GetACLResponse(repBuffer.buffer)
    case `getChildren` => GetChildrenResponse(repBuffer.buffer)
    case `getChildren2` => GetChildren2Response(repBuffer.buffer)
    case `getData` => GetDataResponse(repBuffer.buffer)
    case `multi`=> TransactionResponse(repBuffer.buffer)
    case `setData` => SetDataResponse(repBuffer.buffer)
    case `setACL` => SetACLResponse(repBuffer.buffer)
    case `sync` => SyncResponse(repBuffer.buffer)
    case `setWatches` => ReplyHeader(repBuffer.buffer)
  }
}

object ConnectResponse extends Decoder[ConnectResponse] {
  override def decode(buffer: Buffer): ConnectResponse = {
    val br = BufferReader(buffer)

    // read packet size
    br.readInt
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
  override def decode(buffer: Buffer): CreateResponse = {
    val br = BufferReader(buffer)
    val header = ReplyHeader.decode(br)

    if (header.err == 0)
      new CreateResponse(header, Some(new CreateResponseBody(br.readString)))
    else {
      throw ZookeeperException.create("Error while create", header.err)
    }
  }
}

object ErrorResponse extends Decoder[ErrorResponse] {
  override def decode(buffer: Buffer): ErrorResponse = {
    // TODO Use this
    val br = BufferReader(buffer)
    val header = ReplyHeader.decode(br)

    new ErrorResponse(header, Some(new ErrorResponseBody(br.readInt)))
  }
}

object ExistsResponse extends Decoder[ExistsResponse] {
  override def decode(buffer: Buffer): ExistsResponse = {

    val br = BufferReader(buffer)
    val header = ReplyHeader.decode(br)

    if (header.err == 0)
      new ExistsResponse(header, Some(new ExistsResponseBody(Stat.decode(br))))
    else {
      throw ZookeeperException.create("Error while exists", header.err)
    }
  }
}

object GetACLResponse extends Decoder[GetACLResponse] {
  override def decode(buffer: Buffer): GetACLResponse = {
    val br = BufferReader(buffer)
    val header = ReplyHeader.decode(br)

    if (header.err == 0) {
      val aclList = ACL.decodeArray(br)
      val stat = Stat.decode(br)
      new GetACLResponse(header, Some(new GetACLResponseBody(aclList, stat)))
    }
    else {
      throw ZookeeperException.create("Error while getACL", header.err)
    }
  }
}

object GetChildrenResponse extends Decoder[GetChildrenResponse] {
  override def decode(buffer: Buffer): GetChildrenResponse = {
    val br = BufferReader(buffer)
    val header = ReplyHeader.decode(br)

    if (header.err == 0) {
      val size = br.readInt
      val children = new Array[String](size)

      for (i <- 0 to size - 1) {
        children(i) = br.readString
      }

      new GetChildrenResponse(header, Some(new GetChildrenResponseBody(children)))
    }
    else {
      throw ZookeeperException.create("Error while getChildren", header.err)
    }
  }
}

object GetChildren2Response extends Decoder[GetChildren2Response] {
  override def decode(buffer: Buffer): GetChildren2Response = {
    val br = BufferReader(buffer)
    val header = ReplyHeader.decode(br)

    if (header.err == 0) {
      val size = br.readInt
      val children = new Array[String](size)

      for (i <- 0 to size - 1) {
        children(i) = br.readString
      }

      new GetChildren2Response(header, Some(new GetChildren2ResponseBody(children, Stat.decode(br))))
    }
    else {
      throw ZookeeperException.create("Error while getChildren2", header.err)
    }
  }
}

object GetDataResponse extends Decoder[GetDataResponse] {
  override def decode(buffer: Buffer): GetDataResponse = {
    val br = BufferReader(buffer)
    val h = ReplyHeader.decode(buffer)

    if (h.err == 0) {
      val data = br.readBuffer
      val stat = Stat.decode(br)

      new GetDataResponse(h, Some(new GetDataResponseBody(data, stat)))
    }
    else {
      throw ZookeeperException.create("Error while getData", h.err)
    }
  }
}

object ReplyHeader extends Decoder[ReplyHeader] {
  override def decode(buffer: Buffer): ReplyHeader = {
    val br = BufferReader(buffer)

    br.readInt // Read frame size
    val xid = br.readInt
    val zxid = br.readLong
    val err = br.readInt

    if (err == 0)
      new ReplyHeader(xid, zxid, err)
    else
      throw ZookeeperException.create("Error", err)
  }
}

object SetACLResponse extends Decoder[SetACLResponse] {
  override def decode(buffer: Buffer): SetACLResponse = {
    val br = BufferReader(buffer)
    val header = ReplyHeader.decode(br)

    if (header.err == 0)
      new SetACLResponse(header, Some(new SetACLResponseBody(Stat.decode(br))))
    else {
      throw ZookeeperException.create("Error while setACL", header.err)
    }
  }
}

object SetDataResponse extends Decoder[SetDataResponse] {
  override def decode(buffer: Buffer): SetDataResponse = {
    val br = BufferReader(buffer)
    val header = ReplyHeader.decode(br)

    if (header.err == 0)
      new SetDataResponse(header, Some(new SetDataResponseBody(Stat.decode(br))))
    else {
      throw ZookeeperException.create("Error while setData", header.err)
    }
  }
}

object SyncResponse extends Decoder[SyncResponse] {
  override def decode(buffer: Buffer): SyncResponse = {
    val br = BufferReader(buffer)
    val header = ReplyHeader.decode(br)

    if (header.err == 0)
      new SyncResponse(header, Some(new SyncResponseBody(br.readString)))
    else {
      throw ZookeeperException.create("Error while sync", header.err)
    }
  }
}

object TransactionResponse extends Decoder[TransactionResponse] {
  override def decode(buffer: Buffer): TransactionResponse = {
    val br = BufferReader(buffer)
    val header = ReplyHeader.decode(br)

    if (header.err == 0) {
      new TransactionResponse(header, Transaction.decode(br) )
    } else {
      throw ZookeeperException.create("Error while Transaction", header.err)
    }
  }
}

object WatcherEvent extends Decoder[WatcherEvent] {
  override def decode(buffer: Buffer): WatcherEvent = {
    val br = BufferReader(buffer)
    val header = ReplyHeader.decode(br)

    if (header.err == 0) {
      val typ = br.readInt
      val state = br.readInt
      val path = br.readString

      new WatcherEvent(header, Some(new WatcherEventBody(typ, state, path)))
    }
    else {
      throw ZookeeperException.create("Error while watch event", header.err)
    }
  }
}