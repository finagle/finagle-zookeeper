package com.twitter.finagle.exp.zookeeper.connection

import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.OpCode
import com.twitter.finagle.exp.zookeeper._
import com.twitter.finagle.exp.zookeeper.client.ZkClient
import com.twitter.finagle.exp.zookeeper.data.ACL
import com.twitter.finagle.util.DefaultTimer
import com.twitter.io.Buf
import com.twitter.io.Buf.ByteArray
import com.twitter.util._
import com.twitter.util.TimeConversions._
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.Random

private[finagle] class HostProvider(
  dest: String,
  canBeRO: Boolean,
  timeBetweenPreventiveSearch: Option[Duration],
  timeBetweenRwServerSearch: Option[Duration]
) {

  type TestMethod = String => Future[String]
  val canSearchRwServer = new AtomicBoolean(false)
  var hasStoppedRwServerSearch = Promise[Unit]()
  private[this] var hostList: Seq[String] = HostUtilities.shuffleSeq(HostUtilities.formatHostList(dest))
  var seenRwServer: Option[String] = None
  var seenRoServer: Option[String] = None

  def serverList: Seq[String] = hostList
  private[finagle] def serverList_=(serverList: Seq[String]) = {
    hostList = serverList
  }

  /**
   * Should add the hosts from the list to the host provider's list.
   *
   * @param serverList hosts to be added
   * @return Unit
   */
  def addHost(serverList: String): Unit = this.synchronized {
    val newHosts = HostUtilities.formatAndTest(serverList)
    hostList ++= newHosts.diff(hostList)
    hostList = HostUtilities.shuffleSeq(hostList)
  }

  /**
   * Test if we can connect to this server.
   *
   * @param host the server to test
   * @return Future[Boolean]
   */
  def testHost(host: String): Future[Boolean] = {
    HostUtilities.testIpAddress(host)
    withIsroRequest(host) rescue {
      case exc => withConnectRequest(host)
    } transform {
      case Return(unit) => Future(true)
      case Throw(exc) => Future(false)
    }
  }

  /**
   * Tests if a given server is available and finds a new one it's not.
   *
   * @param host a server to test
   * @return address of available server
   */
  def testOrFind(host: String): Future[String] =
    testHost(host) flatMap { available =>
      if (available) Future(host)
      else findServer()
    }

  /**
   * Find a server to connect to, if no server is found an exception is thrown
   *
   * @throws NoServerFound if no host is found
   * @return Future[String] or Exception
   */
  def findServer(serverList: Option[Seq[String]] = None): Future[String] = {
    // we make priority to seenRwServer over other servers
    val finalHostList = {
      if (serverList.isDefined) {
        serverList.get map HostUtilities.testIpAddress
        HostUtilities.shuffleSeq(serverList.get)
      }
      else {
        if (seenRwServer.isDefined)
          seenRwServer.get +:
            HostUtilities.shuffleSeq(hostList.filter(_ == seenRwServer.get))

        else HostUtilities.shuffleSeq(hostList)
      }
    }
    // Will try to find server using isro request first
    scanHostList(finalHostList)(withIsroRequest) transform {
      // Found a RW server, best choice
      case Return(server) => Future(server)
      // Did not found a RW mode server
      case Throw(exc: NoRwServerFound) =>
        if (canBeRO) {
          seenRoServer match {
            // Found a RO mode server
            case Some(server) => Future(server)
            case None => Future.exception(
              NoServerFound("No server available for connection"))
          }
        } else Future.exception(
          NoServerFound("No RW server available for connection"))

      // isro is maybe request not supported or not allowed
      // will search by connecting to server directly
      case Throw(exc) => scanHostList(finalHostList)(withConnectRequest)
    }
  }

  def startRwServerSearch(): Future[String] = this.synchronized {
    if (!canSearchRwServer.get() && timeBetweenRwServerSearch.isDefined) {
      canSearchRwServer.set(true)
      hasStoppedRwServerSearch = Promise()
      findRwServer(timeBetweenRwServerSearch.get)
    } else Future.exception(
      new RuntimeException("RW mode server search in progress"))
  }

  def stopRwServerSearch(): Future[Unit] = {
    if (!canSearchRwServer.get()) Future.Done
    else {
      canSearchRwServer.set(false)
      hasStoppedRwServerSearch
    }
  }

  /**
   * If we are currently connected to a RO mode server, it will try to search
   * a new RW server every timeInterval until success
   *
   * @param timeInterval Duration between each search attempt
   * @return address of an available RW server
   */
  def findRwServer(timeInterval: Duration): Future[String] =
    scanHostList(HostUtilities.shuffleSeq(hostList))(withIsroRequest) transform {
      // Found a RW server, best choice
      case Return(server) =>
        canSearchRwServer.set(false)
        hasStoppedRwServerSearch.setDone()
        Future(server)

      // isro request not supported, will search by connecting to server
      case Throw(exc: Throwable) =>
        if (canSearchRwServer.get())
          findRwServer(timeInterval)
            .delayed(timeInterval)(DefaultTimer.twitter)
        else {
          hasStoppedRwServerSearch.setDone()
          Future.exception(
            new RuntimeException("RW mode server search interrupted"))
        }
    }

  /**
   * Should go through a host list to find a server using a defined
   * searchMethod.
   *
   * @param hostList server host list
   * @param testMethod take server address and test
   * @return address of an available RW server
   */
  def scanHostList(hostList: Seq[String])(implicit testMethod: TestMethod)
  : Future[String] = hostList match {
    case Seq() =>
      Future.exception(NoServerFound("No server available for connection"))
    case lastServer +: Seq() => testMethod(lastServer) rescue {
      case exc: CouldNotConnect =>
        Future.exception(NoServerFound("No server available for connection"))
      case exc: Throwable => Future.exception(exc)
    }
    case server +: tail => testMethod(server) rescue {
      case exc: NoRwServerFound => scanHostList(tail)
      case exc: CouldNotConnect => scanHostList(tail)
      case exc: Throwable => Future.exception(exc)
    }
  }

  /**
   * Will try to test each host and classify them depending
   * on results.
   *
   * @return Future.Done when the action is completed
   */
  def preventiveRwServerSearch(): Future[Unit] =
    scanHostList(HostUtilities.shuffleSeq(hostList))(withIsroRequest) transform {
      case Return(server) => Future.Done
      case Throw(exc) =>
        scanHostList(hostList)(withConnectRequest).unit rescue {
          case exc2 => Future.Done
        }
    }

  /**
   * Should schedule a preventive search task if not already defined
   */
  def startPreventiveSearch(): Unit = this.synchronized {
    if (timeBetweenPreventiveSearch.isDefined
      && !PreventiveSearchScheduler.isRunning)
      PreventiveSearchScheduler
        .apply(timeBetweenPreventiveSearch.get)(preventiveRwServerSearch())
  }

  /**
   * Should cancel the preventive search task
   * @return Future.Done when the task is cancelled
   */
  def stopPreventiveSearch(): Future[Unit] =
    Future(PreventiveSearchScheduler.stop())

  /**
   * Sends a "isro" request to the server. If the server responds "rw" then
   * it's a Read-Write mode server, if the response is "ro" then the server
   * is in Read-Only mode. If the request does not succeed the server could
   * either not support read only mode (server version < 3.4.0 ) or be down.
   *
   * @param host the server to test
   * @return the tested server address
   */
  def withIsroRequest(host: String): Future[String] = {
    // using BufClient to send unframed requests
    val client = BufClient.newSimpleClient(host)
    // sending "isro" request
    val rep = client.apply() flatMap (_.apply(
      ByteArray("isro".getBytes)
    ))

    rep transform {
      case Return(buf) =>
        val Buf.Utf8(str) = buf.slice(0, buf.length)
        // this is a RW mode server ( connected to majority )
        if (str == "rw") {
          seenRwServer = Some(host)
          ZkClient.logger.info("Found Rw server at %s".format(host))
          Future(host)
        }
        // this is a RO mode server ( not connected to quorum )
        else if (str == "ro") {
          seenRoServer = Some(host)
          ZkClient.logger.info("Found Ro server at %s".format(host))
          Future.exception(NoRwServerFound("This is a RO mode server"))
        }
        else Future.exception(
          NoRwServerFound("No server found with isro request"))

      // isro request not supported by server (server version < 3.4.0)
      case Throw(exc) =>
        Future.exception(
          CouldNotConnect("Could not connect to server : " + host)
            .initCause(exc))

    } ensure client.close()
  }
  /**
   * Sends a connect request followed by close request (if the first one
   * is accepted).
   *
   * @param host the server to test
   * @return the tested server address
   */
  def withConnectRequest(host: String): Future[String] = {
    // we are using BufClient to reduce connection and
    // session management boilerplate for from ZooKeeperClient
    val client = BufClient.newClient(host)
    val service = client.apply()

    def close(): Future[Unit] = service flatMap {
      svc => svc.close() before client.close()
    }

    def serve(buf: Buf): Future[Buf] = service flatMap (_.apply(buf))

    val connectRequest = ReqPacket(
      None,
      Some(ConnectRequest(
        0,
        0L,
        2000.milliseconds,
        0,
        Array[Byte](16),
        canBeRO))
    )

    val closeRequest = ReqPacket(
      Some(RequestHeader(1, OpCode.CLOSE_SESSION)),
      None
    )

    // sending a connect request and then a close request
    val rep = serve(
      Buf.U32BE(connectRequest.buf.length).concat(connectRequest.buf)) flatMap { rep =>
      serve(Buf.U32BE(closeRequest.buf.length).concat(closeRequest.buf))
      val Buf.U32BE(_, rem) = rep
      ConnectResponse(rem) match {
        case Return((body, rem2)) => Future(body)
        case Throw(exc) => Future.exception(exc)
      }
    }

    try {
      rep transform {
        case Return(conRep: ConnectResponse) =>
          if (conRep.isRO) {
            seenRoServer = Some(host)
            ZkClient.logger.info("Found Ro server at %s".format(host))
          }
          else {
            seenRwServer = Some(host)
            ZkClient.logger.info("Found Rw server at %s".format(host))
          }
          Future(host)
        case Throw(exc: Throwable) => Future.exception(
          CouldNotConnect("Could not connect to server : " + host)
            .initCause(exc))
      } ensure close()
    }
    catch {
      case exc: Throwable =>
        Future.exception(
          CouldNotConnect("Could not connect to server : " + host)
            .initCause(exc))
    }
  }

  /**
   * PreventiveSearchScheduler is used to find a suitable server to reconnect
   * to in case of server failure.
   */
  private[this] object PreventiveSearchScheduler {
    /**
     * currentTask - the last scheduled timer's task
     */
    private[this] var currentTask: Option[TimerTask] = None

    def apply(period: Duration)(f: => Unit) {
      currentTask = Some(DefaultTimer.twitter.schedule(period)(f))
    }

    def isRunning: Boolean = { currentTask.isDefined }

    def stop() {
      if (currentTask.isDefined) currentTask.get.cancel()
      currentTask = None
    }

    def updateTimer(period: Duration)(f: => Unit) = {
      stop()
      apply(period)(f)
    }
  }
}

object HostUtilities {
  class ServerNotAvailable(msg: String) extends RuntimeException(msg)

  /**
   * Format original server list by splitting on ',' character
   * and adding in a Sequence.
   *
   * @param list original server list
   * @return a Sequence containing hosts
   */
  def formatHostList(list: String): Seq[String] =
    if (list.trim.isEmpty)
      throw new IllegalArgumentException("Host list is empty")
    else list.trim.split(",")

  def formatAndTest(list: String): Seq[String] = {
    val seq = formatHostList(list)
    seq map HostUtilities.testIpAddress
    seq
  }

  def testIpAddress(address: String) {
    val addressAndPort = address.split(":")
    if (addressAndPort.size != 2)
      throw new IllegalArgumentException(
        "Address %s does not respect this format x.x.x.x:port".format(address))
    else ACL.ipToBytes(addressAndPort(0))
  }

  /**
   * A simple way to shuffle host list, thus we can dispatch clients
   * on several hosts.
   *
   * @param seq sequence to shuffle
   * @tparam T sequence type
   * @return the freshly shuffled sequence
   */
  def shuffleSeq[T](seq: Seq[T]): Seq[T] = seq match {
    case Seq() => Seq[T]()
    case xs =>
      val i = Random.nextInt(xs.size)
      xs(i) +: Random.shuffle(xs.take(i) ++ xs.drop(i + 1))
  }

  object hostStates extends Enumeration {
    type HostState = Value
    val UP, DOWN_1, DOWN_5, DOWN_10, DOWN_30, DOWN_60, DOWN_120, BANNED = Value
  }
}