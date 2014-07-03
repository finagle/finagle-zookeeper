package com.twitter.finagle.exp.zookeeper.connection

import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.exp.zookeeper._
import com.twitter.finagle.exp.zookeeper.connection.HostUtilities.ServerNotAvailable
import com.twitter.util._
import java.util.concurrent.atomic.AtomicBoolean

/**
 * The connection manager is supposed to handle a connection
 * between the client and an endpoint from the host list
 */
class ConnectionManager(
  dest: String,
  canBeRo: Boolean,
  timeForPreventive: Option[Duration],
  timeForRoMode: Option[Duration]
  ) {

  @volatile var connection: Promise[Connection] = Promise[Connection]()
  val isInitiated = new AtomicBoolean(false)
  private[this] var activeHost: Option[String] = None
  private[finagle] val hostProvider = new HostProvider(dest, canBeRo, timeForPreventive, timeForRoMode)

  type SearchMethod = String => Future[ServiceFactory[ReqPacket, RepPacket]]

  /**
   * Should add new hosts to the server list
   * @param hostList the hosts to add
   * @return Future.Done or Exception
   */
  def addHosts(hostList: String): Unit = { hostProvider.addHost(hostList) }

  /**
   * Return the currently connected host
   * @return Some(host) or None if not connected
   */
  def currentHost: Option[String] = activeHost

  /**
   * To close connection manager, the current connexion, stop preventive
   * search and stop rw server search.
   * @return a Future.Done or Exception
   */
  def close(): Future[Unit] =
    connection flatMap { connectn =>
      isInitiated.set(false)
      connectn.close() before hostProvider.stopPreventiveSearch() before
        hostProvider.stopRwServerSearch() before {
        connection = Promise[Connection]()
        Future.Done
      }
    }

  /**
   * To close connection manager, the current connexion, stop preventive
   * search and stop rw server search.
   * @return a Future.Done or Exception
   */
  def close(deadline: Time): Future[Unit] =
    connection flatMap { connectn =>
      isInitiated.set(false)
      connectn.close(deadline) before hostProvider.stopPreventiveSearch() before
        hostProvider.stopRwServerSearch() before {
        connection = Promise[Connection]()
        Future.Done
      }
    }

  /**
   * Should connect to a server with its address
   * @param server the server address
   * @return Future.Done
   */
  private[this] def connect(server: String): Future[Unit] =
    if (connection.isDefined) {
      connection flatMap (_.close())
      activeHost = Some(server)
      connection = Promise[Connection]()
      connection.setValue(
        new Connection(ZooKeeperClient.newClient(server)))
      isInitiated.set(true)
      Future.Done
    } else {
      activeHost = Some(server)
      connection.setValue(
        new Connection(ZooKeeperClient.newClient(server)))
      isInitiated.set(true)
      Future.Done
    }


  /**
   * Find a server, and connect to it, priority to RW server,
   * then RO server and finally not RO server.
   * @return a Future.Done or Exception
   */
  def findAndConnect(): Future[Unit] =
    hostProvider.findServer() transform {
      case Return(server) => connect(server)
      case Throw(exc) => Future.exception(exc)
    }

  def testAndConnect(host: String): Future[Unit] =
    hostProvider.testHost(host) transform {
      case Return(available) =>
        if (available) connect(host)
        else Future.exception(new ServerNotAvailable(
          "%s is not available for connection".format(host)))
      case Throw(exc) => Future.exception(exc)
    }

  /**
   * Should say if the current connection is valid or not
   * @return Future[Boolean]
   */
  private[finagle] def hasAvailableConnection: Future[Boolean] =
    if (connection.isDefined) {
      connection flatMap { connectn =>
        if (connectn.isServiceFactoryAvailable
          && connectn.isValid.get())
          connectn.isServiceAvailable
        else Future(false)
      }
    } else Future(false)


  /**
   * Initiates connection Manager on client creation
   * @return Future.Done or Exception
   */
  def initConnectionManager(): Future[Unit] =
    if (!isInitiated.get())
      hostProvider.findServer() flatMap { server =>
        connection.setValue(new Connection(ZooKeeperClient.newClient(server)))
        hostProvider.startPreventiveSearch()
        isInitiated.set(true)
        Future.Unit
      }
    else Future.exception(
      new RuntimeException("ConnectionManager is already initiated"))

  /**
   * Connect to a new server from host list
   * @return Future.Done or Exception
   */
  def reconnect(): Future[Unit] =
    if (isInitiated.get()) {
      findAndConnect() flatMap { _ =>
        Future(hostProvider.startPreventiveSearch())
      }
    } else
      Future.exception(
        new RuntimeException("ConnectionManager is not initiated"))

  /**
   * Should remove the given hosts from the host list, if the client is
   * connected to one of these hosts, we need to disconnect before and
   * connect to an available server.
   * @param hostList the hosts to remove from the current host list.
   * @return Future.Done or NoServerFound exception
   */
  def removeHosts(hostList: String): Future[String] = {
    val hostSeq = hostProvider.formatHostList(hostList)
    if (currentHost.isDefined && hostSeq.contains(currentHost.get)) {
      val newList = hostProvider.serverList filterNot (hostList.contains(_))

      hostProvider.findServer(Some(newList))
    } else Future(activeHost.get)
  }

  /**
   * Reset connection manager, restart preventive search,
   * connect to a new server.
   * @return Future.Done or Exception
   */
  /*def resetAndReconnect(): Future[Unit] =
    if (isInitiated.get() && connection.isDefined) {
      hostProvider.stopPreventiveSearch()
      hostProvider.stopRwServerSearch()
      hostProvider.seenRoServer = None
      hostProvider.seenRwServer = None
      isInitiated.set(true)
      reconnect()
    } else
      Future.exception(
        new RuntimeException("ConnectionManager is not initiated"))*/

}