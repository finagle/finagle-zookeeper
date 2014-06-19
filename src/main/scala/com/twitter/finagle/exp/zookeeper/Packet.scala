package com.twitter.finagle.exp.zookeeper

import com.twitter.io.Buf

case class ReqPacket(header: Option[RequestHeader], request: Option[Request]) {

  def buf: Buf = header match {
      case Some(requestHeader) =>
        request match {
          case Some(req: Request) =>
            requestHeader.buf
              .concat(req.buf)
          case None => requestHeader.buf
        }
      case None =>
        request  match {
          case Some(req: ConnectRequest) => req.buf
          case _ => throw new IllegalArgumentException("Not expected body in ReqPacket")
        }
    }
}

case class RepPacket(header: StateHeader, response: Option[Response])

case class RequestRecord(opCode: Int, xid: Option[Int])

case class StateHeader(err: Int, zxid: Long)
object StateHeader {
  def apply(header: ReplyHeader): StateHeader = new StateHeader(header.err, header.zxid)
}