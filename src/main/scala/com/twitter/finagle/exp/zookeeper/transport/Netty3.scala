package com.twitter.finagle.exp.zookeeper.transport

import org.jboss.netty.channel._
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.netty3.Netty3Transporter
import com.twitter.util.NonFatal
import org.jboss.netty.buffer.ChannelBuffers._

/**
 * A Netty3 pipeline that is responsible for framing network
 * traffic in terms of mysql logical packets.
 */

object PipelineFactory extends ChannelPipelineFactory {
  override def getPipeline: ChannelPipeline = {
    // Maybe packet formatting is too heavy or incorrect
    val pipeline = Channels.pipeline()
    pipeline.addLast("packetEncoder", new PacketEncoder)
    pipeline
  }
}

/**
 * Responsible for the transport layer plumbing required to produce
 * a Transport[Packet, Packet]. The current implementation uses
 * Netty3.
 */

object ZooKeeperTransporter extends Netty3Transporter[ChannelBuffer, ChannelBuffer](
  "zookeeper",
  PipelineFactory
)

/**
 * When sending packet, this method is called
 */

class PacketEncoder extends SimpleChannelDownstreamHandler {
  // TODO: log in logger with debug level
  override def writeRequested(ctx: ChannelHandlerContext, evt: MessageEvent) =
    evt.getMessage match {
      case ch: ChannelBuffer =>
        try {
          val bb = ch.toByteBuffer

          // Write the packet size at the beginning and rewind
          bb.putInt(bb.capacity() - 4)
          bb.rewind()

          Channels.write(ctx, evt.getFuture, wrappedBuffer(bb), evt.getRemoteAddress)
        } catch {
          case NonFatal(e) =>
            evt.getFuture.setFailure(new ChannelException(e.getMessage))
        }

      case unknown =>
        evt.getFuture.setFailure(new ChannelException(
          "Unsupported write request type %s".format(unknown.getClass.getName)))
    }
}
