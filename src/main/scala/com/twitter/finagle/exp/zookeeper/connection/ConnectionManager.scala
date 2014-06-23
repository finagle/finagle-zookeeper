package com.twitter.finagle.exp.zookeeper.connection

import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.OpCode
import com.twitter.finagle.exp.zookeeper._
import com.twitter.finagle.exp.zookeeper.transport.BufInt
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.util.DefaultTimer
import com.twitter.io.Buf
import com.twitter.io.Buf.ByteArray
import com.twitter.util._
import com.twitter.util.TimeConversions._

/**
 * The connection manager is supposed to handle the connection
 * whether it is in standalone or quorum mode
 */
class ConnectionManager(
  timeBetweenPrevntvSrch: Option[Duration],
  dest: String
  ) {

  val hostList: Seq[String] = formatHostList(dest)
  var seenRwServer: Option[String] = None
  var seenRoServer: Option[String] = None
  @volatile var connection: Option[Connection] = None
  type SearchMethod = String => Future[ServiceFactory[ReqPacket, RepPacket]]

  /**
   * This will try to find a Read-Write mode server by sending isro request
   * with BufClient ( can't use ZooKeeperClient to send this request, because
   * the Zktransport is framing every request, however isro request must not be
   * framed ).
   * If no server is found, maybe isro request is not supported (ie server version < 3.3.0)
   * this will try to connect to each server and find a good one.
   * @return future client ServiceFactory
   */
  def findNextServer: Future[ServiceFactory[ReqPacket, RepPacket]] = {
    findServer(hostList)(withRwRequest) transform {
      // Found a RW server, best choice
      case Return(serviceFactory) => Future(serviceFactory)

      // Did not found a RW mode server
      case Throw(exc: NoRwServerFound) =>
        seenRoServer match {
          case Some(server) =>
            // Found a RO mode server
            Future(ZooKeeperClient.newClient(server))
          case None =>
            // No server found
            Future.exception(NoServerFound("No server available for connection"))
        }

      // isro request not supported, will search by connecting to server
      case Throw(exc: Throwable) => findServer(hostList)(withConnectRequest)
    }
  }

  def findServer(hostList: Seq[String])(implicit searchMethod: SearchMethod):
  Future[ServiceFactory[ReqPacket, RepPacket]] = hostList match {
    case Seq() =>
      Future.exception(NoServerFound("No server available for connection"))
    case lastServer +: Seq() => searchMethod(lastServer) rescue {
      case exc: CouldNotConnect =>
        Future.exception(NoServerFound("No server available for connection"))
      case exc: Throwable => Future.exception(exc)
    }
    case server +: tail => searchMethod(server) rescue {
      case exc: NoRwServerFound => findServer(tail)
      case exc: CouldNotConnect => findServer(tail)
      case exc: Throwable => Future.exception(exc)
    }
  }

  def initConnection(): Future[Unit] = {
    findNextServer flatMap { factory =>
      connection = Some(new Connection(factory))
      if(timeBetweenPrevntvSrch.isDefined)
        PreventiveSearchScheduler.apply(timeBetweenPrevntvSrch.get)(preventiveRwServerSearch())
      Future.Unit
    }
  }

  def preventiveRwServerSearch(): Future[Unit] = {
    findServer(hostList)(withRwRequest) transform {
      // Found a RW server, best choice
      // the RW server address is in seenRwServer value
      case Return(serviceFactory) => serviceFactory.close()

      // todo isro maybe not supported use session connection
      case Throw(exc: Throwable) => Future.Unit
    }
  }

  def searchRwServer(timeInterval: Duration):
  Future[ServiceFactory[ReqPacket, RepPacket]] = {
    findServer(hostList)(withRwRequest) transform {
      // Found a RW server, best choice
      case Return(serviceFactory) => Future(serviceFactory)

      // isro request not supported, will search by connecting to server
      case Throw(exc: Throwable) => searchRwServer(timeInterval).delayed(timeInterval)(DefaultTimer.twitter)
    }
  }

  def withRwRequest(server: String):
  Future[ServiceFactory[ReqPacket, RepPacket]] = {
    val client = BufClient.newSimpleClient(server)
    // sending "isro" request
    val rep = client.apply() flatMap (_.apply(
      ByteArray("isro".getBytes)
    ))

    rep transform {
      case Return(buf) =>
        // reading response
        val Buf.Utf8(str) = buf.slice(0, buf.length)
        // this is a RW mode server ( connected to majority )
        if (str == "rw") {
          seenRwServer = Some(server)
          Future(ZooKeeperClient.newClient(server))
        }
        // this is a RO mode server ( not connected to quorum )
        else if (str == "ro") {
          seenRoServer = Some(server)
          Future.exception(NoRwServerFound("This is a RO mode server"))
        } ensure client.close()
        else {
          // Case not supposed to exist
          Future.exception(NoRwServerFound(""))
        } ensure client.close()

      // isro request not supported by server (server version <= 3.3.0)
      case Throw(exc) => {
        Future.exception(CouldNotConnect("Could not connect to server : " + server)
          .initCause(exc))
      } ensure client.close()
    }
  }

  def withConnectRequest(server: String):
  Future[ServiceFactory[ReqPacket, RepPacket]] = {
    // will try to connect to server and close session, return new client if succeed
    // we are using BufClient to avoid connection and session management boilerplate
    try {
      val client = BufClient.newClient(server)
      val service = client.apply()
      def serve(buf: Buf): Future[Unit] = service flatMap (_.apply(buf).unit)
      val conReq = ReqPacket(
        None,
        Some(new ConnectRequest(0, 0L, 2000.milliseconds, canBeRO = true)))
      val closeReq = ReqPacket(Some(RequestHeader(1, OpCode.CLOSE_SESSION)), None)

      val rep = serve(BufInt(conReq.buf.length).concat(conReq.buf)) before {
        serve(BufInt(closeReq.buf.length).concat(closeReq.buf))
      }

      rep transform {
        case Return(unit) =>
          client.close() before Future(ZooKeeper.newClient(server))
        case Throw(exc: Throwable) =>
          client.close() before
            Future.exception(CouldNotConnect("Could not connect to server : " + server)
              .initCause(exc))
      }
    }
    catch {
      case exc: Throwable =>
        Future.exception(CouldNotConnect("Could not connect to server : " + server)
          .initCause(exc))
    }
  }

  def formatHostList(list: String): Seq[String] =
    if (list.trim.isEmpty)
      throw new IllegalArgumentException("Host list is empty")
    else {
      list.trim.split(",")
    }

  /**
   * PreventiveSearchScheduler is used to find a suitable server to reconnect to
   * in case of deconnection.
   */
  object PreventiveSearchScheduler {
    /**
     * Local variables
     * timer - default Twitter timer ( never stop it! )
     * currentTask - the last scheduled timer's task
     */
    val timer = DefaultTimer
    var currentTask: Option[TimerTask] = None

    def apply(period: Duration)(f: => Unit) {
      currentTask = Some(timer.twitter.schedule(period)(f))
    }

    def updateTimer(period: Duration)(f: => Unit) = {
      currentTask.get.cancel()
      apply(period)(f)
    }
  }
}