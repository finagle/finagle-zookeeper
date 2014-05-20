package com.twitter.finagle.exp.zookeeper

import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.exp.zookeeper.transport.{BufferReader, BufferWriter, Buffer}
import com.twitter.util.Try
import com.twitter.finagle.exp.zookeeper.ZookeeperDefinitions.opCode


sealed trait OpResult
sealed trait OpRequest {
  val toChannelBuffer: ChannelBuffer
}

trait ResultDecoder[U <: OpResult] extends (BufferReader => Try[U]) {
  def apply(buffer: BufferReader): Try[U] = Try(decode(buffer))
  def decode(buffer: BufferReader): U
}

class Transaction(opList: Array[OpRequest]) {
  val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(0))

    opList foreach {
      case create: CreateOp =>
        val multiHeader = new MultiHeader(opCode.create, false, -1)
        bw.write(multiHeader.toChannelBuffer)
        bw.write(create.toChannelBuffer)

      case delete: DeleteOp =>
        val multiHeader = new MultiHeader(opCode.delete, false, -1)
        bw.write(multiHeader.toChannelBuffer)
        bw.write(delete.toChannelBuffer)

      case setData: SetDataOp =>
        val multiHeader = new MultiHeader(opCode.setData, false, -1)
        bw.write(multiHeader.toChannelBuffer)
        bw.write(setData.toChannelBuffer)

      case check: CheckOp =>
        val multiHeader = new MultiHeader(opCode.check, false, -1)
        bw.write(multiHeader.toChannelBuffer)
        bw.write(check.toChannelBuffer)

      case _ => throw new RuntimeException("Request not supported for Transaction")
    }

    val lastOne = new MultiHeader(-1, true, -1)
    bw.write(lastOne.toChannelBuffer)

    bw.underlying
  }
}

object Transaction {
  def decode(buffer: BufferReader): Array[OpResult] = {
    val operations = collection.mutable.ArrayBuffer[OpResult]()

    //TODO maybe synchronize this
    var h = MultiHeader.decode(buffer)

    while (!h.state) {
      if (h.err == 0) {
        h.typ match {
          case opCode.create => operations += CreateOp.decode(buffer)
          case opCode.delete => operations += DeleteOp.decode(buffer)
          case opCode.setData => operations += SetDataOp.decode(buffer)
          case opCode.check => operations += CheckOp.decode(buffer)
        }
        h = MultiHeader.decode(buffer)
      } else {
        throw ZookeeperException.create("Error while transaction ", h.err)
      }
    }
    operations.toArray
  }
}

case class MultiHeader(typ: Int, state: Boolean, err: Int)
  extends OpRequest {
  override val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(0))

    bw.write(typ)
    bw.write(state)
    bw.write(err)

    bw.underlying
  }
}

object MultiHeader {
  def decode(buffer: BufferReader): MultiHeader = {
    new MultiHeader(buffer.readInt, buffer.readBool, buffer.readInt)
  }
}

case class CreateResult(typ: Int, path: String) extends OpResult
case class DeleteResult(typ: Int) extends OpResult
case class SetDataResult(typ: Int, stat: Stat) extends OpResult
case class CheckResult(typ: Int) extends OpResult

case class CreateOp(
  path: String,
  data: Array[Byte],
  aclList: Array[ACL],
  createMode: Int)
  extends OpRequest {
  override val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(0))

    bw.write(path)
    bw.write(data)
    bw.write(aclList)
    bw.write(createMode)

    bw.underlying
  }
}

object CreateOp extends ResultDecoder[CreateResult] {
  override def decode(buffer: BufferReader): CreateResult = {
    new CreateResult(opCode.create, buffer.readString)
  }
}

case class DeleteOp(path: String, version: Int)
  extends OpRequest {
  val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(4))

    bw.write(path)
    bw.write(version)

    bw.underlying
  }
}

object DeleteOp extends ResultDecoder[DeleteResult] {
  override def decode(buffer: BufferReader): DeleteResult = {
    new DeleteResult(buffer.readInt)
  }
}

case class SetDataOp(path: String, data: Array[Byte], version: Int)
  extends OpRequest {
  override val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(4))

    bw.write(path)
    bw.write(data)
    bw.write(version)

    bw.underlying
  }
}

object SetDataOp extends ResultDecoder[SetDataResult] {
  override def decode(buffer: BufferReader): SetDataResult = {
    new SetDataResult(buffer.readInt, Stat.decode(buffer))
  }
}

case class CheckOp(path: String, version: Int)
  extends OpRequest {
  override val toChannelBuffer: ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(0))

    bw.write(path)
    bw.write(version)

    bw.underlying
  }
}

object CheckOp extends ResultDecoder[CheckResult] {
  override def decode(buffer: BufferReader): CheckResult = {
    new CheckResult(buffer.readInt)
  }
}