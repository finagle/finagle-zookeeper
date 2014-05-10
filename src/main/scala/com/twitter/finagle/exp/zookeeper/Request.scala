package com.twitter.finagle.exp.zookeeper

import com.twitter.finagle.exp.zookeeper.transport.{BufferWriter, Buffer}
import org.jboss.netty.buffer.ChannelBuffers._
import scala.Some
import org.jboss.netty.buffer.ChannelBuffer

trait Request {
  val toChannelBuffer: ChannelBuffer
}

trait Body {
  val toChannelBuffer: ChannelBuffer
}

case class ConnectRequest(protocolVersion: Int = 0,
                          lastZxidSeen: Long = 0L,
                          timeOut: Int = 2000,
                          sessionId: Long = 0L,
                          passwd: Array[Byte] = new Array[Byte](16),
                          canBeRO: Option[Boolean] = Some(true)) extends Request {

  override val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(0))

    bw.write(-1)
    bw.write(protocolVersion)
    bw.write(lastZxidSeen)
    bw.write(timeOut)
    bw.write(sessionId)
    bw.write(passwd.length)
    bw.write(passwd)
    bw.write(canBeRO.getOrElse(false))

    bw.underlying.copy
  }
}

case class RequestHeader(xid: Int, opCode: Int) extends Request {
  override val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(0))

    bw.write(-1)
    bw.write(xid)
    bw.write(opCode)

    bw.underlying.copy
  }
}

case class CreateRequestBody(path: String, data: Array[Byte], aclListe: Array[ACL], createMode: Int) extends Body {
  val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(0))

    bw.write(path)
    bw.write(data)
    bw.write(aclListe)
    bw.write(createMode)

    bw.underlying
  }
}

case class CreateRequest(header: RequestHeader, body: CreateRequestBody) extends Request {
  override val toChannelBuffer: ChannelBuffer = wrappedBuffer(header.toChannelBuffer, body.toChannelBuffer)
}

case class GetACLRequestBody(path: String) extends Body {
  val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(path.length))

    bw.write(path)
    bw.underlying
  }
}

case class GetACLRequest(header: RequestHeader, body: GetACLRequestBody) extends Request {
  val toChannelBuffer: ChannelBuffer = wrappedBuffer(header.toChannelBuffer, body.toChannelBuffer)
}

case class GetDataRequestBody(path: String, watcher: Boolean) extends Body {
  val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(2 * path.length))

    bw.write(path)
    bw.write(watcher)

    bw.underlying
  }
}

case class GetDataRequest(header: RequestHeader, body: GetDataRequestBody) extends Request {
  override val toChannelBuffer: ChannelBuffer = wrappedBuffer(header.toChannelBuffer, body.toChannelBuffer)
}


case class DeleteRequestBody(path: String, version: Int) extends Body {
  val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(4))

    bw.write(path)
    bw.write(version)

    bw.underlying
  }
}

case class DeleteRequest(header: RequestHeader, body: DeleteRequestBody) extends Request {
  override val toChannelBuffer: ChannelBuffer = wrappedBuffer(header.toChannelBuffer, body.toChannelBuffer)
}

case class ExistsRequestBody(path: String, watch: Boolean) extends Body {
  val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(4))

    bw.write(path)
    bw.write(watch)

    bw.underlying
  }
}

case class ExistsRequest(header: RequestHeader, body: ExistsRequestBody) extends Request {
  override val toChannelBuffer: ChannelBuffer = wrappedBuffer(header.toChannelBuffer, body.toChannelBuffer)
}

case class SetDataRequestBody(path: String, data: Array[Byte], version: Int) extends Body {
  val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(4))

    bw.write(path)
    bw.write(data)
    bw.write(version)

    bw.underlying
  }
}

case class SetDataRequest(header: RequestHeader, body: SetDataRequestBody) extends Request {
  override val toChannelBuffer: ChannelBuffer = wrappedBuffer(header.toChannelBuffer, body.toChannelBuffer)
}

case class GetChildrenRequestBody(path: String, watch: Boolean) extends Body {
  override val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(2))

    bw.write(path)
    bw.write(watch)

    bw.underlying
  }
}

case class GetChildrenRequest(header: RequestHeader, body: GetChildrenRequestBody) extends Request {
  override val toChannelBuffer: ChannelBuffer = wrappedBuffer(header.toChannelBuffer, body.toChannelBuffer)
}

case class GetChildren2RequestBody(path: String, watch: Boolean) extends Body {
  override val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(2))

    bw.write(path)
    bw.write(watch)

    bw.underlying
  }
}

case class GetChildren2Request(header: RequestHeader, body: GetChildren2RequestBody) extends Request {
  override val toChannelBuffer: ChannelBuffer = wrappedBuffer(header.toChannelBuffer, body.toChannelBuffer)
}

case class SetACLRequestBody(path: String, acl: Array[ACL], version: Int) extends Body {
  override val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(4))

    bw.write(path)
    bw.write(acl)
    bw.write(version)

    bw.underlying
  }
}

case class SetACLRequest(header: RequestHeader, body: SetACLRequestBody) extends Request {
  override val toChannelBuffer: ChannelBuffer = wrappedBuffer(header.toChannelBuffer, body.toChannelBuffer)
}

case class SyncRequestBody(path: String) extends Body {
  override val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(4))

    bw.write(path)

    bw.underlying
  }
}

case class SyncRequest(header: RequestHeader, body: SyncRequestBody) extends Request {
  override val toChannelBuffer: ChannelBuffer = wrappedBuffer(header.toChannelBuffer, body.toChannelBuffer)
}

case class SetWatchesRequestBody(relativeZxid: Int, dataWatches: Array[String], existsWatches: Array[String], childWatches: Array[String]) extends Body {
  override val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(4))

    bw.write(relativeZxid)
    bw.write(dataWatches)
    bw.write(existsWatches)
    bw.write(childWatches)

    bw.underlying
  }
}

case class SetWatchesRequest(header: RequestHeader, body: SetWatchesRequestBody) extends Request {
  override val toChannelBuffer: ChannelBuffer = wrappedBuffer(header.toChannelBuffer, body.toChannelBuffer)
}