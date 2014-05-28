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

case class RequestHeader(xid: Int, opCode: Int){
  val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(0))

    bw.write(-1)
    bw.write(xid)
    bw.write(opCode)

    bw.underlying.copy
  }
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
  path: String,
  data: Array[Byte],
  aclList: Array[ACL],
  createMode: Int)
  extends Request {

  val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(0))

    bw.write(path)
    bw.write(data)
    bw.write(aclList)
    bw.write(createMode)

    bw.underlying.copy()
  }
}


case class GetACLRequest(path: String)
  extends Request {
  val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(path.length))

    bw.write(path)
    bw.underlying.copy()
  }
}

case class GetDataRequest(path: String, watcher: Boolean)
  extends Request {
  val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(2 * path.length))

    bw.write(path)
    bw.write(watcher)

    bw.underlying.copy()
  }
}

case class GetMaxChildrenRequestBody(path: String)
  extends Request {
  override val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(0))

    bw.write(path)

    bw.underlying.copy()
  }
}

case class DeleteRequest(path: String, version: Int)
  extends Request {
  val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(4))

    bw.write(path)
    bw.write(version)

    bw.underlying.copy()
  }
}

case class ExistsRequest(path: String, watch: Boolean)
  extends Request {
  val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(4))

    bw.write(path)
    bw.write(watch)

    bw.underlying.copy()
  }
}

class PingRequest extends RequestHeader(2, opCode.ping) with Request {
  override val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(4))

    bw.write(-1)
    bw.write(xid)
    bw.write(opCode)

    bw.underlying.copy()
  }
}
class CloseSessionRequest extends RequestHeader(1, opCode.ping) with Request {
  override val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(4))

    bw.write(-1)
    bw.write(xid)
    bw.write(opCode)

    bw.underlying.copy()
  }
}

case class SetDataRequest(
  path: String,
  data: Array[Byte],
  version: Int)
  extends Request {
  val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(4))

    bw.write(path)
    bw.write(data)
    bw.write(version)

    bw.underlying.copy()
  }
}

case class GetChildrenRequest(path: String, watch: Boolean)
  extends Request {
  val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(4))

    bw.write(path)
    bw.write(watch)

    bw.underlying.copy()
  }
}

case class GetChildren2Request(path: String, watch: Boolean)
  extends Request {
  val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(4))

    bw.write(path)
    bw.write(watch)

    bw.underlying.copy()
  }
}

case class SetACLRequest(path: String, acl: Array[ACL], version: Int)
  extends Request {
  val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(4))

    bw.write(path)
    bw.write(acl)
    bw.write(version)

    bw.underlying.copy()
  }
}

case class SetMaxChildrenRequest(path: String, max: Int)
  extends Request {
  val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(4))

    bw.write(path)
    bw.write(max)

    bw.underlying.copy()
  }
}

case class SyncRequest(path: String)
  extends Request {
  val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(4))

    bw.write(path)

    bw.underlying.copy()
  }
}

case class SetWatchesRequest(
  relativeZxid: Int,
  dataWatches: Array[String],
  existsWatches: Array[String],
  childWatches: Array[String])
  extends Request {
  val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(4))

    bw.write(relativeZxid)
    bw.write(dataWatches)
    bw.write(existsWatches)
    bw.write(childWatches)

    bw.underlying.copy()
  }
}

case class TransactionRequest(transaction: Transaction)
  extends Request {
  val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(4))

    bw.write(transaction.toChannelBuffer)

    bw.underlying.copy()
  }
}