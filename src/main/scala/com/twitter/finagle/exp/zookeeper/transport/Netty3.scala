package com.twitter.finagle.exp.zookeeper.transport

import com.twitter.finagle.Stack
import com.twitter.finagle.netty3.Netty3Transporter
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel._

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

object ZookeeperTransporter {
  def apply(params: Stack.Params) = Netty3Transporter[ChannelBuffer, ChannelBuffer](PipelineFactory, params)
}