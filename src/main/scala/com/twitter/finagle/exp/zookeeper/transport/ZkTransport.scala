package com.twitter.finagle.exp.zookeeper.transport

import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.exp.zookeeper._
import com.twitter.util.{Future, Time}
import java.net.SocketAddress
import com.twitter.finagle.exp.zookeeper.ConnectRequest
import scala.collection.mutable

case class ZkTransport(
  trans: Transport[ChannelBuffer, ChannelBuffer]
  ) extends Transport[Request, Response] {
  val stack = new mutable.SynchronizedQueue[Request]

  /**
   * When receiving a Request
   * @param req the Request to encode
   * @return Future[Unit] when the message is correctly written
   */
  override def write(req: Request): Future[Unit] = req match {
    case re: ConnectRequest => doWrite(re)
    case re: RequestHeader => doWrite(re)
  }

  override def read(): Future[Response] = {
    stack.front match {
      case rep: ConnectRequest => trans.read flatMap { buffer =>
        val connectRep = ConnectResponse.decode(Buffer.fromChannelBuffer(buffer))
        Future.value(connectRep)
      }
      case rep: RequestHeader => trans.read flatMap { buffer =>
        val replyRep = ReplyHeader.decode(Buffer.fromChannelBuffer(buffer))
        Future.value(replyRep)
      }
      case _ => throw new RuntimeException("READ REQUEST NOT SUPPORTED")
    }

  }

  def doWrite(req: Request): Future[Unit] = {
    stack.enqueue(req)
    trans.write(req.toChannelBuffer)
  }

  override def remoteAddress: SocketAddress = trans.remoteAddress
  override def localAddress: SocketAddress = trans.localAddress
  override def isOpen: Boolean = trans.isOpen
  override val onClose: Future[Throwable] = trans.onClose
  override def close(deadline: Time): Future[Unit] = trans.close(deadline)
}
