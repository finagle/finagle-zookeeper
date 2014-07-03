package com.twitter.finagle.exp.zookeeper

import com.twitter.io.Buf

/**
 * A ReqPacket is a simple abstraction designed to represent what a request
 * can be. A ReqPacket can be composed of a Header and/or a Request, or None.
 * Examples:
 * - connect : ReqPacket(None, Some(Request))
 * - closeSession : ReqPacket(Some(RequestHeader), None)
 * - create : ReqPacket(Some(RequestHeader), Some(Request))
 * - configureDispatcher : ReqPacket(None, None)
 * @param header optionally Some(RequestHeader) or None
 * @param request optionally Some(Request) or None
 */
private[finagle]
case class ReqPacket(header: Option[RequestHeader], request: Option[Request]) {
  def buf: Buf = if (header.isDefined) {
    request match {
      case Some(req: Request) => header.get.buf.concat(req.buf)
      case None => header.get.buf
    }
  } else {
    request match {
      case Some(req: ConnectRequest) => req.buf
      case _ => throw new IllegalArgumentException(
        "Packet not allowed : no header + request")
    }
  }
}

/**
 * Used to represent a response, composed by a StateHeader
 * and an Option[Response].
 * @param err request's error code
 * @param response an optional Response
 */
private[finagle]
case class RepPacket(err: Option[Int], response: Option[Response])