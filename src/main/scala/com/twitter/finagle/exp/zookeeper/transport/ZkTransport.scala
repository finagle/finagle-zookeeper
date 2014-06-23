package com.twitter.finagle.exp.zookeeper.transport

import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.transport.Transport
import com.twitter.io.Buf
import com.twitter.util.{Future, Time}
import java.net.SocketAddress
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}

class ZkTransport(trans: Transport[ChannelBuffer, ChannelBuffer])
  extends Transport[Buf, Buf] {

  @volatile private[this] var buf = Buf.Empty
  def remoteAddress: SocketAddress = trans.remoteAddress
  def localAddress: SocketAddress = trans.localAddress
  def isOpen: Boolean = trans.isOpen
  val onClose: Future[Throwable] = trans.onClose
  def close(deadline: Time): Future[Unit] = trans.close(deadline)

  def write(req: Buf): Future[Unit] = {
    val framedReq = BufInt(req.length).concat(req)
    val bytes = new Array[Byte](framedReq.length)
    framedReq.write(bytes, 0)
    trans.write(ChannelBuffers.wrappedBuffer(bytes))
  }

  /**
   * We are using this dedicated transport because we want to read
   * incoming buffers one at a time. Every time read is called,
   * we check if the queue is not empty, if not we give the front frame,
   * if it's empty we read every frame until there is nothing more to read.
   */
  def read(): Future[Buf] =
    read(4) flatMap { case BufInt(len, _) => read(len) }

  /*if (len < 0 || len >= ClientCnxn.packetLen) {
    throw new IOException("Packet len" + len + " is out of range!");
  }*/

  private[this] def read(len: Int): Future[Buf] =
    if (buf.length < len) {
      trans.read flatMap { chanBuf =>
        buf = buf.concat(ChannelBufferBuf(chanBuf))
        read(len)
      }
    } else {
      val out = buf.slice(0, len)
      buf = buf.slice(len, buf.length)
      Future.value(out)
    }
}

class BufTransport(trans: Transport[ChannelBuffer, ChannelBuffer])
  extends Transport[Buf, Buf] {

  def remoteAddress: SocketAddress = trans.remoteAddress
  def localAddress: SocketAddress = trans.localAddress
  def isOpen: Boolean = trans.isOpen
  val onClose: Future[Throwable] = trans.onClose
  def close(deadline: Time): Future[Unit] = trans.close(deadline)

  def write(req: Buf): Future[Unit] = {
    val bytes = new Array[Byte](req.length)
    req.write(bytes, 0)
    trans.write(ChannelBuffers.wrappedBuffer(bytes))
  }

  def read(): Future[Buf] =
    trans.read flatMap { chanBuf =>
      Future(ChannelBufferBuf(chanBuf))
    }
}