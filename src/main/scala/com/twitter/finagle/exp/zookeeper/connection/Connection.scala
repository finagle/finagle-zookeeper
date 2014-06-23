package com.twitter.finagle.exp.zookeeper.connection

import com.twitter.finagle.exp.zookeeper.{RepPacket, ReqPacket}
import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.util.{Duration, Future, Time}
import java.util.concurrent.atomic.AtomicBoolean

class Connection(serviceFactory: ServiceFactory[ReqPacket, RepPacket]) {
  @volatile private[this] var service: Future[Service[ReqPacket, RepPacket]] = serviceFactory.apply()
  val isValid = new AtomicBoolean(true)
  /**
   * Is set to true when a connection to a r/w server is established for the
   * first time; never changed afterwards.
   * <p>
   * Is used to handle situations when client without sessionId connects to a
   * read-only server. Such client receives "fake" sessionId from read-only
   * server, but this sessionId is invalid for other servers. So when such
   * client finds a r/w server, it sends 0 instead of fake sessionId during
   * connection handshake and establishes new, valid session.
   * <p>
   * If this field is false (which implies we haven't seen r/w server before)
   * then non-zero sessionId is fake, otherwise it is valid.
   */
  @volatile var seenRwServerBefore: Boolean = false
  //val ZooKeeperSaslClient zooKeeperSaslClient

  def close(): Future[Unit] = serviceFactory.close()
  def close(time: Time): Future[Unit] = serviceFactory.close(time)
  def close(duration: Duration): Future[Unit] = serviceFactory.close(duration)

  def newService() { service = serviceFactory.apply() }
  def serve(req: ReqPacket): Future[RepPacket] = service flatMap (_(req))
}
