package com.twitter.finagle.exp.zookeeper.connection

import com.twitter.finagle.exp.zookeeper.{RepPacket, ReqPacket}
import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.util.{Duration, Future, Time}
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Connection manages a ServiceFactory, in charge of serving requests to server
 *
 * @param serviceFactory current connection to server
 */
class Connection(serviceFactory: ServiceFactory[ReqPacket, RepPacket]) {
  val isValid = new AtomicBoolean(true)
  private[this] val service: Future[Service[ReqPacket, RepPacket]] =
    serviceFactory.apply()

  /**
   * Close current service and ServiceFactory
   *
   * @return Future.Done
   */
  def close(): Future[Unit] = {
    service flatMap { svc =>
      isValid.set(false)
      if (svc.isAvailable && serviceFactory.isAvailable) {
        svc.close() before serviceFactory.close()
      } else if (svc.isAvailable && !serviceFactory.isAvailable) {
        svc.close()
      } else if (!svc.isAvailable && serviceFactory.isAvailable) {
        serviceFactory.close()
      } else {
        Future.Done
      }
    }
  }
  def close(time: Time): Future[Unit] = {
    service flatMap { svc =>
      isValid.set(false)
      if (svc.isAvailable && serviceFactory.isAvailable) {
        svc.close(time) before serviceFactory.close(time)
      } else if (svc.isAvailable && !serviceFactory.isAvailable) {
        svc.close(time)
      } else if (!svc.isAvailable && serviceFactory.isAvailable) {
        serviceFactory.close(time)
      } else {
        Future.Done
      }
    }
  }
  def close(duration: Duration): Future[Unit] = {
    service flatMap { svc =>
      isValid.set(false)
      if (svc.isAvailable && serviceFactory.isAvailable) {
        svc.close(duration) before serviceFactory.close(duration)
      } else if (svc.isAvailable && !serviceFactory.isAvailable) {
        svc.close(duration)
      } else if (!svc.isAvailable && serviceFactory.isAvailable) {
        serviceFactory.close(duration)
      } else {
        Future.Done
      }
    }
  }
  def isServiceFactoryAvailable: Boolean = serviceFactory.isAvailable
  def isServiceAvailable: Future[Boolean] = service flatMap
    (svc => Future(svc.isAvailable))
  def serve(req: ReqPacket): Future[RepPacket] = service flatMap (_(req))
}

private[finagle] object Connection {
  class NoConnectionAvailable(msg: String) extends RuntimeException(msg)
}