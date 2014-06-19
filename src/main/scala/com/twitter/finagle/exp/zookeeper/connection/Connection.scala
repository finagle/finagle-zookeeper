package com.twitter.finagle.exp.zookeeper.connection

import com.twitter.finagle.exp.zookeeper.{RepPacket, ReqPacket}
import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.util.{Duration, Future, Time}
import java.util.concurrent.atomic.AtomicBoolean

class Connection(serviceFactory: ServiceFactory[ReqPacket, RepPacket]) {
  @volatile private[this] var service: Future[Service[ReqPacket, RepPacket]] = serviceFactory.apply()
  val isValid = new AtomicBoolean(true)

  def close(): Future[Unit] = serviceFactory.close()
  def close(time: Time): Future[Unit] = serviceFactory.close(time)
  def close(duration: Duration): Future[Unit] = serviceFactory.close(duration)

  def serve(req: ReqPacket): Future[RepPacket] = service flatMap( _(req))
}
