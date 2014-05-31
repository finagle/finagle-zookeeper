package com.twitter.finagle.exp.zookeeper

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers._

case class Packet(header: Option[RequestHeader], request: Option[Request]) {

  def serialize: ChannelBuffer = {

    header match {
      case Some(requestHeader: RequestHeader) =>
        request match {
          case Some(req: Request) => wrappedBuffer(requestHeader.toChannelBuffer, req.toChannelBuffer)
          case None => wrappedBuffer(requestHeader.toChannelBuffer)
        }
      case None =>
        request match {
          case Some(connectRequest: ConnectRequest) =>
            wrappedBuffer(connectRequest.toChannelBuffer)
        }
    }
  }
}

case class RequestRecord(opCode: Int, xid: Option[Int])