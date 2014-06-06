package com.twitter.finagle.exp.zookeeper.transport

import org.jboss.netty.channel._
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.netty3.Netty3Transporter

/**
 * A Netty3 pipeline that is responsible for framing network
 * traffic in terms of mysql logical packets.
 */

object PipelineFactory extends ChannelPipelineFactory {
  def getPipeline = Channels.pipeline()
}

/**
 * Responsible for the transport layer plumbing required to produce
 * a Transport[Packet, Packet]. The current implementation uses
 * Netty3.
 */

object NettyTrans extends Netty3Transporter[ChannelBuffer, ChannelBuffer](
  "zookeeper", PipelineFactory)
