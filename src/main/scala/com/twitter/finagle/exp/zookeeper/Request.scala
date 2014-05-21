package com.twitter.finagle.exp.zookeeper

import com.twitter.finagle.exp.zookeeper.transport.{BufferWriter, Buffer}
import org.jboss.netty.buffer.ChannelBuffers._
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.exp.zookeeper.ZookeeperDefinitions.opCode

/**
 * Same as the Response type, a Request can be composed by a header or
 * by body + header.
 *
 * However ConnectRequest is an exception, it is only composed of a body.
 */

sealed trait Request {
  val toChannelBuffer: ChannelBuffer
}

trait RequestHeader {
  val xid: Int
  val opCode: Int
}

case class ConnectRequest(protocolVersion: Int = 0,
  lastZxidSeen: Long = 0L,
  timeOut: Int = 2000,
  sessionId: Long = 0L,
  passwd: Array[Byte] = Array[Byte](16),
  canBeRO: Option[Boolean] = Some(true))
  extends Request {

  override val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(0))

    bw.write(-1)
    bw.write(protocolVersion)
    bw.write(lastZxidSeen)
    bw.write(timeOut)
    bw.write(sessionId)
    bw.write(passwd)
    bw.write(canBeRO.getOrElse(false))

    bw.underlying.copy()
  }
}

case class CreateRequest(
  xid: Int,
  opCode: Int,
  path: String,
  data: Array[Byte],
  aclList: Array[ACL],
  createMode: Int)
  extends RequestHeader with Request {

  val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(0))

    bw.write(-1)
    bw.write(xid)
    bw.write(opCode)

    bw.write(path)
    bw.write(data)
    bw.write(aclList)
    bw.write(createMode)

    bw.underlying.copy()
  }
}

case class CloseSessionRequest(xid: Int, opCode: Int)
  extends RequestHeader with Request {
  override val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(12))

    bw.write(-1)
    bw.write(xid)
    bw.write(opCode)

    bw.underlying.copy()
  }
}

case class GetACLRequest(
  xid: Int,
  opCode: Int,
  path: String)
  extends RequestHeader with Request {
  val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(path.length))

    bw.write(-1)
    bw.write(xid)
    bw.write(opCode)

    bw.write(path)
    bw.underlying.copy()
  }
}

case class GetDataRequest(
  xid: Int,
  opCode: Int,
  path: String,
  watcher: Boolean)
  extends RequestHeader with Request {
  val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(2 * path.length))

    bw.write(-1)
    bw.write(xid)
    bw.write(opCode)

    bw.write(path)
    bw.write(watcher)

    bw.underlying.copy()
  }
}

case class GetMaxChildrenRequestBody(
  xid: Int,
  opCode: Int,
  path: String)
  extends RequestHeader with Request {
  override val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(0))

    bw.write(-1)
    bw.write(xid)
    bw.write(opCode)

    bw.write(path)

    bw.underlying.copy()
  }
}

case class DeleteRequest(
  xid: Int,
  opCode: Int,
  path: String,
  version: Int)
  extends RequestHeader with Request {
  val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(4))

    bw.write(-1)
    bw.write(xid)
    bw.write(opCode)

    bw.write(path)
    bw.write(version)

    bw.underlying.copy()
  }
}

case class ExistsRequest(
  xid: Int,
  opCode: Int,
  path: String,
  watch: Boolean)
  extends RequestHeader with Request {
  val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(4))

    bw.write(-1)
    bw.write(xid)
    bw.write(opCode)

    bw.write(path)
    bw.write(watch)

    bw.underlying.copy()
  }
}

class PingRequest
  extends RequestHeader with Request {
  override val xid: Int = -2
  override val opCode: Int = ZookeeperDefinitions.opCode.ping
  override val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(0))


    bw.write(-1)
    bw.write(xid) //xid
    bw.write(opCode) //OpCode

    bw.underlying.copy()
  }
}

case class SetDataRequest(
  xid: Int,
  opCode: Int,
  path: String,
  data: Array[Byte],
  version: Int)
  extends RequestHeader with Request {
  val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(4))

    bw.write(-1)
    bw.write(xid)
    bw.write(opCode)

    bw.write(path)
    bw.write(data)
    bw.write(version)

    bw.underlying.copy()
  }
}

case class GetChildrenRequest(
  xid: Int,
  opCode: Int,
  path: String,
  watch: Boolean)
  extends RequestHeader with Request {
  val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(4))

    bw.write(-1)
    bw.write(xid)
    bw.write(opCode)

    bw.write(path)
    bw.write(watch)

    bw.underlying.copy()
  }
}

case class GetChildren2Request(
  xid: Int,
  opCode: Int,
  path: String,
  watch: Boolean)
  extends RequestHeader with Request {
  val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(4))

    bw.write(-1)
    bw.write(xid)
    bw.write(opCode)

    bw.write(path)
    bw.write(watch)

    bw.underlying.copy()
  }
}

case class SetACLRequest(
  xid: Int,
  opCode: Int,
  path: String,
  acl: Array[ACL],
  version: Int)
  extends RequestHeader with Request {
  val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(4))

    bw.write(-1)
    bw.write(xid)
    bw.write(opCode)

    bw.write(path)
    bw.write(acl)
    bw.write(version)

    bw.underlying.copy()
  }
}

case class SetMaxChildrenRequest(
  xid: Int,
  opCode: Int,
  path: String,
  max: Int)
  extends RequestHeader with Request {
  val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(4))

    bw.write(-1)
    bw.write(xid)
    bw.write(opCode)

    bw.write(path)
    bw.write(max)

    bw.underlying.copy()
  }
}

case class SyncRequest(
  xid: Int,
  opCode: Int,
  path: String)
  extends RequestHeader with Request {
  val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(4))

    bw.write(-1)
    bw.write(xid)
    bw.write(opCode)

    bw.write(path)

    bw.underlying.copy()
  }
}

case class SetWatchesRequest(
  xid: Int,
  opCode: Int,
  relativeZxid: Int,
  dataWatches: Array[String],
  existsWatches: Array[String],
  childWatches: Array[String])
  extends RequestHeader with Request {
  val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(4))

    bw.write(-1)
    bw.write(xid)
    bw.write(opCode)

    bw.write(relativeZxid)
    bw.write(dataWatches)
    bw.write(existsWatches)
    bw.write(childWatches)

    bw.underlying.copy()
  }
}

case class TransactionRequest(
  xid: Int,
  opCode: Int,
  transaction: Transaction)
  extends RequestHeader with Request {
  val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(4))

    bw.write(-1)
    bw.write(xid)
    bw.write(opCode)

    bw.write(transaction.toChannelBuffer)

    bw.underlying.copy()
  }
}