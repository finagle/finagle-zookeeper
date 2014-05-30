package com.twitter.finagle.exp.zookeeper.transport

import org.jboss.netty.buffer.{ChannelBufferFactory, ChannelBuffer}
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Throw, Return, Future, Time}
import java.net.SocketAddress
import scala.collection.mutable

class ZkTransport(trans: Transport[ChannelBuffer, ChannelBuffer])
  extends Transport[ChannelBuffer, ChannelBuffer] {
  val bufferQueue = new mutable.SynchronizedQueue[ChannelBuffer]
  def write(req: ChannelBuffer): Future[Unit] = trans.write(req)

  /**
   * We are using this dedicated transport because we want to read
   * incoming buffers one at a time. Every time read is called, we try
   * to read a buffer from the original transport, if there are multiple
   * frames in one buffer, then we split each frames, and put them in the queue.
   *
   * If there is nothing to read, we first check that the queue is not empty,
   * and it is, the corresponding exception is thrown.
   */
  def read(): Future[ChannelBuffer] = {
    /*trans.read() respond  {
      case Return(buffer) =>

        /**
         * 1/ Read packet size
         * 2/ Copy corresponding buffer to the queue
         * 3/ If the packet is not completely read (ie reader index != writer index) go to 1/
         * 4/ When reader index == writer index send bufferQueue.dequeue
         */
        val br = BufferReader(buffer)
        println("Before Reader %d, Writer %d".format(br.underlying.readerIndex(), br.underlying.writerIndex()))
        while (br.underlying.readerIndex() != br.underlying.writerIndex())
          bufferQueue.enqueue(br.readFrame)

        println("After Reader %d, Writer %d".format(br.underlying.readerIndex(), br.underlying.writerIndex()))
        val buf = bufferQueue.dequeue()
        println(buf.readerIndex()+" "+buf.writerIndex())


      case Throw(exc) =>

        if (!bufferQueue.isEmpty)
          bufferQueue.dequeue()
        else
          throw exc

      /**
       * 1/ Check if the bufferQueue is not empty
       * 2/ If not send bufferQueue.dequeue
       * 3/ If yes send Future.exception(exc)
       */
    }*/

    trans.read() flatMap { buffer =>
      val br = BufferReader(buffer)
      while (br.underlying.readerIndex() != br.underlying.writerIndex())
        bufferQueue.enqueue(br.readFrame)

      Future(bufferQueue.dequeue())
    } rescue { case exc =>
      if (!bufferQueue.isEmpty)
        Future(bufferQueue.dequeue())
      else
        throw throw exc
    }
  }


  def remoteAddress: SocketAddress = trans.remoteAddress
  def localAddress: SocketAddress = trans.localAddress
  def isOpen: Boolean = trans.isOpen
  val onClose: Future[Throwable] = trans.onClose
  def close(deadline: Time): Future[Unit] = trans.close(deadline)
}
