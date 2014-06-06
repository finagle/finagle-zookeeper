package com.twitter.finagle.exp.zookeeper.client

import com.twitter.finagle.transport.Transport
import com.twitter.util._
import com.twitter.finagle.exp.zookeeper._
import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.io.Buf

class ZkDispatcher(trans: Transport[Buf, Buf])
  extends GenSerialClientDispatcher[Request, Response, Buf, Buf](trans) {
  //we give the processor apply, thus it can send/read Req-Rep cf: Ping
  val requestMatcher = new RequestMatcher(trans, apply)

  protected def dispatch(req: Request, p: Promise[Response]): Future[Unit] = {
    requestMatcher.write(req) respond { resp =>
      p.updateIfEmpty(resp)
    }
  }.unit
}