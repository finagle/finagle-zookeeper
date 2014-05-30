package com.twitter.finagle.exp.zookeeper.transport

import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import java.nio.charset.StandardCharsets
import org.jboss.netty.buffer.ChannelBuffers._
import java.nio.{ByteBuffer, ByteOrder}
import com.twitter.finagle.exp.zookeeper.{ID, ACL}

/**
 * BufferReader reads Buffer to type T
 * BufferWriter writes [T] to Buffer
 */

object Buffer {
  def apply(bytes: Array[Byte]): Buffer = new Buffer {
    // a wrappedBuffer should avoid copying the arrays.
    val underlying = wrappedBuffer(ByteOrder.BIG_ENDIAN, bytes)
  }

  def apply(bufs: Buffer*): Buffer = {
    val underlying = wrappedBuffer(bufs.map(_.underlying): _*)
    fromChannelBuffer(underlying)
  }

  def getDynamicBuffer(size: Int) = new Buffer {
    val underlying = ChannelBuffers.dynamicBuffer(ByteOrder.BIG_ENDIAN, size)
  }

  def fromChannelBuffer(cb: ChannelBuffer): Buffer = {
    require(cb != null)
    require(cb.order == ByteOrder.BIG_ENDIAN, "Invalid ChannelBuffer ByteOrder")
    new Buffer {
      val underlying = cb
    }
  }
}

trait Buffer {
  val underlying: ChannelBuffer
  def capacity: Int = underlying.capacity
  def bufferSize: Int = underlying.readableBytes()
}


trait BufferReader extends Buffer {
  def readByte: Byte
  def readBool: Boolean
  def readInt: Int
  def readLong: Long
  def readFloat: Float
  def readDouble: Double
  def readFrame: ChannelBuffer
  def readShort: Short
  def readString: String
  def readBuffer: Array[Byte]
}

object BufferFactory {
  def apply(channelBuffer: ChannelBuffer): Buffer = {
    new Buffer {
      override val underlying: ChannelBuffer = channelBuffer
    }
  }
}

object BufferReader {

  def apply(buf: Buffer, offset: Int = 0): BufferReader = {
    require(offset >= 0, "Invalid reader offset")
    buf.underlying.readerIndex(offset)
    new Netty3BufferReader(buf.underlying)
  }

  def apply(buffer: ChannelBuffer): BufferReader = {
    new Netty3BufferReader(buffer)
  }

  def apply(bytes: Array[Byte]): BufferReader =
    apply(Buffer(bytes), 0)

  private[this] class Netty3BufferReader(val underlying: ChannelBuffer)
    extends BufferReader with Buffer {

    def readFrame: ChannelBuffer = {
      val length = underlying.readInt()
      underlying.readBytes(length).copy()
    }

    def readBuffer: Array[Byte] = {
      val length = underlying.readInt

      length match {
        case _ if (length == 0 || length == -1) => new Array[Byte](0)
        case _ if length < 0 => throw new IllegalArgumentException
        case _ if length > 0 =>
          val buffer = new Array[Byte](length)
          underlying.readBytes(buffer, 0, length)
          buffer
      }
    }

    def readString: String = {
      val len = underlying.readInt()
      len match {
        case -1 => throw new IllegalThreadStateException
        case _ =>
          val buffer = new Array[Byte](len)
          underlying.readBytes(buffer, 0, len)
          new String(buffer, StandardCharsets.UTF_8)
      }
    }

    def readDouble: Double = underlying.readDouble()
    def readFloat: Float = underlying.readFloat
    def readLong: Long = underlying.readLong
    def readInt: Int = underlying.readInt
    def readShort: Short = underlying.readShort
    def readByte: Byte = underlying.readByte
    def readBool: Boolean = underlying.readByte match {
      case 1 => true
      case 0 => false
    }
  }

}

trait BufferWriter extends Buffer {
  def write[A](value: A): BufferWriter
}

object BufferWriter {

  def apply(buf: Buffer, offset: Int = 0): BufferWriter = {
    require(offset >= 0, "Invalid writer offset.")
    buf.underlying.writerIndex(offset)
    new Netty3BufferWriter(buf.underlying)
  }

  def apply(bytes: Array[Byte]): BufferWriter =
    apply(Buffer(bytes), 0)


  private[this] class Netty3BufferWriter(val underlying: ChannelBuffer)
    extends BufferWriter {

    def write[A](value: A): BufferWriter = value match {
      case b: Byte => underlying.writeByte(b); this
      case i: Int => underlying.writeInt(i); this
      case f: Float => underlying.writeFloat(f); this
      case d: Double => underlying.writeDouble(d); this
      case s: Short => underlying.writeShort(s); this
      case l: Long => underlying.writeLong(l); this
      case b: Boolean => underlying.writeByte(if (b) 1 else 0); this
      case str: String =>
        if (str == null)
          write(-1)
        else {
          write(str.getBytes(StandardCharsets.UTF_8))
        }
        this
      case channelBuf: ChannelBuffer => underlying.writeBytes(channelBuf); this
      case acl: ACL =>
        write(acl.perms)
        write(acl.id)
      case id: ID =>
        write(id.scheme)
        write(id.id)
      case tab: Array[A] =>
        if (tab == null || tab.size == 0)
          write(0)
        else {
          write(tab.length)
          tab.foreach(e => write(e))
        }
        this
      case byteBuffer: ByteBuffer =>
        underlying.writeBytes(byteBuffer)
        this
      case some:Some[A] => write(some.get)

      case null => write(-1)
      case _ => throw new IllegalArgumentException
    }
  }

}
