package com.twitter.finagle.exp.zookeeper.transport

import com.twitter.finagle.Status
import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.transport.Transport
import com.twitter.io.Buf
import com.twitter.util.{Future, Time}
import java.io.IOException
import java.net.SocketAddress
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}

/**
 * ZkTransport:
 * - on writing : frame outgoing Buf, convert to ChannelBuffer and
 * write on given next Transport.
 * - on reading : read packet size, and copy corresponding buffer into Buf
 * @param trans ChannelBuffer Transport
 */
private[finagle] class ZkTransport(trans: Transport[ChannelBuffer, ChannelBuffer])
  extends Transport[Buf, Buf] {

  @volatile var buf = Buf.Empty
  def close(deadline: Time): Future[Unit] = trans.close(deadline)
  def status: Status = trans.status
  def localAddress: SocketAddress = trans.localAddress
  val maxBuffer: Int = 4096 * 1024
  val onClose: Future[Throwable] = trans.onClose
  def remoteAddress: SocketAddress = trans.remoteAddress

  def read(): Future[Buf] = this.synchronized {
    read(4) flatMap {
      case Buf.U32BE(len, _) if len < 0 || len >= maxBuffer =>
        // Emptying buffer before throwing
        buf = Buf.Empty
        Future.exception(new IOException("Packet len" + len + " is out of range!"))
      case Buf.U32BE(len, _) => read(len)
    }
  }

  def read(len: Int): Future[Buf] =
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

  def write(req: Buf): Future[Unit] = {
    val framedReq = Buf.U32BE(req.length).concat(req)
    val bytes = new Array[Byte](framedReq.length)
    framedReq.write(bytes, 0)
    trans.write(ChannelBuffers.wrappedBuffer(bytes))
  }
}

/**
 * BufTransport:
 * - on writing : convert to ChannelBuffer and write on given next Transport.
 * - on reading : copy ChannelBuffer to Buf
 * @param trans ChannelBuffer Transport
 */
private[finagle] class BufTransport(trans: Transport[ChannelBuffer, ChannelBuffer])
  extends Transport[Buf, Buf] {

  def close(deadline: Time): Future[Unit] = trans.close(deadline)
  def status: Status = trans.status
  def localAddress: SocketAddress = trans.localAddress
  val onClose: Future[Throwable] = trans.onClose
  def remoteAddress: SocketAddress = trans.remoteAddress

  def read(): Future[Buf] =
    trans.read flatMap { chanBuf => Future(ChannelBufferBuf(chanBuf)) }

  def write(req: Buf): Future[Unit] = {
    val bytes = new Array[Byte](req.length)
    req.write(bytes, 0)
    trans.write(ChannelBuffers.wrappedBuffer(bytes))
  }
}