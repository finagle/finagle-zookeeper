package com.twitter.finagle.exp.zookeeper.connection

import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.finagle.exp.zookeeper.{RepPacket, ReqPacket, Response, Request}
import com.twitter.util.{Await, Duration, Time, Future}

class Connection(serviceFactory: ServiceFactory[ReqPacket, RepPacket]) {
  @volatile private[this] var service: Service[ReqPacket, RepPacket] = Await.result(serviceFactory.apply())
  def close(): Future[Unit] = serviceFactory.close()
  def close(time: Time): Future[Unit] = serviceFactory.close(time)
  def close(duration: Duration): Future[Unit] = serviceFactory.close(duration)

  def serve(req: ReqPacket): Future[RepPacket] = service(req)
}
