package com.twitter.finagle.exp.zookeeper.connection

import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.exp.zookeeper._
import com.twitter.finagle.exp.zookeeper.client.ZkClient
import com.twitter.finagle.exp.zookeeper.connection.HostUtilities.ServerNotAvailable
import com.twitter.util._
import java.util.concurrent.atomic.AtomicBoolean

/**
 * The connection manager is supposed to handle a connection
 * between the client and an endpoint from the host list
 */
class ConnectionManager(
  dest: String,
  label: Option[String],
  canBeRo: Boolean,
  timeForPreventive: Option[Duration],
  timeForRoMode: Option[Duration]
) {

  @volatile var connection: Option[Connection] = None
  val isInitiated = new AtomicBoolean(false)
  private[this] var activeHost: Option[String] = None
  private[finagle] val hostProvider =
    new HostProvider(dest, canBeRo, timeForPreventive, timeForRoMode)

  type SearchMethod = String => Future[ServiceFactory[ReqPacket, RepPacket]]

  /**
   * Return the currently connected host
   *
   * @return Some(host) or None if not connected
   */
  def currentHost: Option[String] = activeHost

  /**
   * To close connection manager, the current connexion, stop preventive
   * search and stop rw server search.
   *
   * @return a Future.Done or Exception
   */
  def close(): Future[Unit] = {
    if (connection.isDefined) {
      isInitiated.set(false)
      hostProvider.stopPreventiveSearch()
      connection.get.close()
    } else {
      isInitiated.set(false)
      hostProvider.stopPreventiveSearch()
      Future.Done
    }
  }

  /**
   * To close connection manager, the current connexion, stop preventive
   * search and stop rw server search.
   *
   * @return a Future.Done or Exception
   */
  def close(deadline: Time): Future[Unit] = {
    isInitiated.set(false)
    if (connection.isDefined) connection.get.close(deadline)
    else Future.Done
  }

  /**
   * Should connect to a server with its address.
   *
   * @param server the server address
   */
  private[this] def connect(server: String): Unit = this.synchronized {
    if (connection.isDefined) connection.get.close()
    activeHost = Some(server)
    connection = Some(new Connection(newServiceFactory(server)))
    isInitiated.set(true)

    ZkClient.logger.info(s"Now connected to $server")
  }

  /**
   * Find a server, and connect to it, priority to RW server,
   * then RO server and finally not RO server.
   *
   * @return a Future.Done or Exception
   */
  def findAndConnect(hostList: Option[Seq[String]] = None): Future[Unit] =
    hostProvider.findServer(hostList) transform {
      case Return(server) => Future(connect(server))
      case Throw(exc) => Future.exception(exc)
    }

  /**
   * Creates a new service factory.
   * @param server the server to connect to
   * @return a brand new service factory
   */
  def newServiceFactory(server: String): ServiceFactory[ReqPacket, RepPacket] =
    if (label.isDefined) ZookeeperStackClient.newClient(server, label.get)
    else ZookeeperStackClient.newClient(server)

  /**
   * Test a server with isro request and connect request, then connect to it
   * if testing is successful.
   *
   * @param host a host to test
   * @return Future.Done or Exception
   */
  def testAndConnect(host: String): Future[Unit] =
    hostProvider.testHost(host) transform {
      case Return(available) =>
        if (available) {
          hostProvider.addHost(host)
          connect(host)
          Future.Done
        }
        else Future.exception(new ServerNotAvailable(
          "%s is not available for connection".format(host)))
      case Throw(exc) => Future.exception(exc)
    }

  /**
   * Should say if we have a service factory
   *
   * @return Future[Boolean]
   */
  private[finagle] def hasAvailableServiceFactory: Boolean = {
    connection match {
      case Some(connect) =>
        connect.isServiceFactoryAvailable && connect.isValid.get()
      case None => false
    }
  }

  /**
   * Should say if we have a valid Service
   *
   * @return Future[Boolean]
   */
  private[finagle] def hasAvailableService: Future[Boolean] = {
    connection match {
      case Some(connect) =>
        if (connect.isServiceFactoryAvailable
          && connect.isValid.get()) connect.isServiceAvailable
        else Future(false)
      case None => Future(false)
    }
  }

  /**
   * Initiates connection Manager on client creation.
   *
   * @return Future.Done or Exception
   */
  def initConnectionManager(): Future[Unit] =
    if (!isInitiated.get())
      hostProvider.findServer() flatMap { server =>
        connect(server)
        hostProvider.startPreventiveSearch()
        Future.Unit
      }
    else Future.exception(
      new RuntimeException("ConnectionManager is already initiated"))
}