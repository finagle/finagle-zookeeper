package com.twitter.finagle.exp.zookeeper.client

import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.OpCode
import com.twitter.finagle.exp.zookeeper._
import com.twitter.finagle.exp.zookeeper.connection.ConnectionManager
import com.twitter.finagle.exp.zookeeper.session.Session.States
import com.twitter.finagle.exp.zookeeper.session.SessionManager
import com.twitter.finagle.exp.zookeeper.watch.{Watch, WatchManager}
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{CancelledRequestException, ChannelException, WriteException}
import com.twitter.io.Buf
import com.twitter.util._
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable

class ResponseMatcher(trans: Transport[Buf, Buf]) {
  sealed case class ResponsePacket(header: Option[ReplyHeader], body: Option[Response])
  sealed case class RequestRecord(opCode: Int, xid: Option[Int])

  /**
   * Local variables
   * processesReq - queue of requests waiting for responses
   * watchManager - the session watch manager
   * isReading - the reading loop is started or not
   *
   * encoder - creates a complete Packet from a request
   * decoder - creates a response from a Buffer
   */
  private[this] val processedReq = new mutable.SynchronizedQueue[(RequestRecord, Promise[RepPacket])]

  private[this] var connectionManager: Option[ConnectionManager] = None
  private[this] var sessionManager: Option[SessionManager] = None
  private[this] var watchManager: Option[WatchManager] = None

  private[this] val isReading = new AtomicBoolean(false)
  private[this] val hasDispatcherFailed = new AtomicBoolean(false)
  private[this] val encoder = new Writer(trans)
  private[this] val decoder = new Reader(trans)

  def read(): Future[Unit] = {
    val fullRep = if (!hasDispatcherFailed.get) {
      trans.read() transform {
        case Return(buffer) =>
          val currentReq: Option[(RequestRecord, Promise[RepPacket])] =
            if (processedReq.size > 0) Some(processedReq.front) else None

          decoder.read(currentReq, buffer) rescue {
            // If this exception is associated to a request, then propagate to the promise
            case exc => if (currentReq.isDefined) {
              processedReq.dequeue()._2.setException(exc)
              Future.exception(exc)
            } else Future.exception(exc)
          }

        case Throw(exc) => exc match {
          case exc: Exception
            if exc.isInstanceOf[ChannelException]
              | exc.isInstanceOf[WriteException] =>
            failDispatcher(exc)
            Future.exception(new CancelledRequestException(exc))

          case exc: Exception => Future.exception(new CancelledRequestException(exc))
        }
      }
    } else Future.exception(new CancelledRequestException)

    fullRep transform {
      case Return(rep) => rep.body match {
        // This is a notification
        case Some(event: WatchEvent) => Future.Done

        // This is a Response with Body
        case Some(resp: Response) =>
          // The request record is dequeued now that it's satisfied
          processedReq.dequeue()._2.setValue(RepPacket(
          { rep.header map (header => Some(header.err)) getOrElse None },
          rep.body))
          Future.Done

        // This is a Response without Body
        case None =>
          processedReq.dequeue()._2.setValue(RepPacket(
          { rep.header map (header => Some(header.err)) getOrElse None },
          rep.body))
          Future.Done
      }

      // This case is already handled during decoder.read (see onFailure)
      case Throw(exc) => Future.Done
    }
  }

  def readLoop(): Future[Unit] = {
    if (sessionManager.isDefined &&
      //!sessionManager.get.session.isClosingSession.get &&
      !hasDispatcherFailed.get) read() before readLoop()
    else {
      isReading.set(false)
      Future.Done
    }
  }

  /**
   * We make decisions depending on request type,
   * a request record of the Packet is added to the queue, then the packet is
   * finally written to the transport.
   * @param req the request to send
   * @return a Future[Unit] when the request is finally written
   */
  def write(req: ReqPacket): Future[RepPacket] = req match {
    // Dispatcher configuration request
    case ReqPacket(None,
    Some(ConfigureRequest(conMngr, sessMngr, watchMngr))) =>
      connectionManager = Some(conMngr)
      sessionManager = Some(sessMngr)
      watchManager = Some(watchMngr)
      Future(new RepPacket(None, Some(new EmptyResponse)))

    // ZooKeeper Request
    case ReqPacket(_, _) =>
      if (!hasDispatcherFailed.get) {
        val reqRecord = req match {
          case ReqPacket(Some(header), _) =>
            RequestRecord(header.opCode, Some(header.xid))
          // if no header, this is a connect request
          case ReqPacket(None, Some(req: ConnectRequest)) =>
            RequestRecord(OpCode.CREATE_SESSION, None)
        }
        // The request is about to be written, so we add it to the queue of pending request
        val p = new Promise[RepPacket]()

        synchronized {
          processedReq.enqueue((reqRecord, p))
          encoder.write(req) flatMap { unit =>
            if (!isReading.getAndSet(true)) {
              readLoop()
            }
            p
          }
        }

      } else Future.exception(new CancelledRequestException)
  }

  /**
   * Checks an association between a request and a ReplyHeader
   * @param reqRecord expected request details
   * @param repHeader freshly decoded ReplyHeader
   */
  def checkAssociation(reqRecord: RequestRecord, repHeader: ReplyHeader): Unit = {
    if (reqRecord.xid.isDefined)
      if (reqRecord.xid.get != repHeader.xid)
        throw new ZkDispatchingException("wrong association")

  }

  class Reader(trans: Transport[Buf, Buf]) {

    /**
     * This function will try to decode the buffer depending if a request
     * is waiting for a response or not.
     * In case there is a request, it will try to decode with readFromRequest,
     * if an exception is thrown then it will try again with readFromHeader.
     * @param pendingRequest expected request description
     * @param buffer current buffer
     * @return
     */
    def read(
      pendingRequest: Option[(RequestRecord, Promise[RepPacket])],
      buffer: Buf): Future[ResponsePacket] = {
      /**
       * if Some(record) then a request is waiting for a response
       * if None then this is a watchEvent
       */
      val response = pendingRequest match {
        case Some(record) => readFromRequest(record, buffer)
        case None => readNotification(buffer)
      }

      response match {
        case Return(rep) =>
          rep flatMap { resp =>
            if (resp.header.isDefined)
              sessionManager.get.parseHeader(resp.header.get)
            Future(resp)
          }

        case Throw(exc1) => exc1 match {
          case exc: Exception if exc.isInstanceOf[ZkDecodingException]
            | exc.isInstanceOf[ZkDispatchingException] =>

            readNotification(buffer) match {
              case Return(watch) => watch
              case Throw(exc2) =>
                Future.exception(ZkDecodingException(
                  "Impossible to decode this response").initCause(exc2))
            }

          case serverExc: ZookeeperException => Future.exception(exc1)
          case exc => throw exc
        }
      }
    }

    def readNotification(buf: Buf): Try[Future[ResponsePacket]] = Try {
      val (header, rem) = ReplyHeader(buf) match {
        case Return((header@ReplyHeader(_, _, 0), buf2)) => (header, buf2)
        case Return((header@ReplyHeader(_, _, err), buf2)) =>
          throw ZookeeperException.create("Error while readFromHeader :", err)
        case Throw(exc) => throw ZkDecodingException(
          "Error while decoding header").initCause(exc)
      }

      header.xid match {
        case -1 =>
          WatchEvent(rem) match {
            case Return((event@WatchEvent(_, _, _), rem2)) =>
              // check session state
              sessionManager.get.parseWatchEvent(event)
              // Notifies the watch manager we have a new watchEvent
              watchManager.get.process(event)
              val packet = ResponsePacket(Some(header), Some(event))
              Future(packet)
            case Throw(exc) =>
              Future.exception(ZkDecodingException(
                "Error while decoding watch event").initCause(exc))
          }

        case _ =>
          Future.exception(ZkDecodingException("Could not decode this Buf"))
      }
    }

    /**
     * If a request is expected, this function will be called first
     * the expected response's opCode is matched, so that we have the
     * correct way to decode. The header is decoded first, the resulting
     * header is checked with checkAssociation to make sure both xids
     * are equal.
     * The body is decoded next and sent with the header in a Future
     *
     * If any exception is thrown then the readFromHeader will be called.
     * @param req expected request
     * @param buf current buffer reader
     * @return possibly the (header, response) or an exception
     */
    def readFromRequest(
      req: (RequestRecord, Promise[RepPacket]),
      buf: Buf): Try[Future[ResponsePacket]] = Try {

      val (reqRecord, _) = req
      reqRecord.opCode match {
        case OpCode.AUTH =>
          ReplyHeader(buf) match {
            case Return((header, rem)) =>
              checkAssociation(reqRecord, header)
              // if Auth failed we need to clear watches
              if (header.err == -115) watchManager.get.process(
                WatchEvent(Watch.EventType.NONE, Watch.State.AUTH_FAILED, ""))
              Future(ResponsePacket(Some(header), None))
            case Throw(exc) => throw exc
          }

        case OpCode.CREATE_SESSION =>
          ConnectResponse(buf) match {
            case Return((body, rem)) => Future(ResponsePacket(None, Some(body)))
            case Throw(exception) => throw exception
          }

        case OpCode.PING => decodeHeader(reqRecord, buf)
        case OpCode.CLOSE_SESSION => decodeHeader(reqRecord, buf)
        case OpCode.CREATE => decodeResponse(reqRecord, buf, CreateResponse.apply)
        case OpCode.EXISTS => decodeResponse(reqRecord, buf, ExistsResponse.apply)
        case OpCode.DELETE => decodeHeader(reqRecord, buf)
        case OpCode.SET_DATA => decodeResponse(reqRecord, buf, SetDataResponse.apply)
        case OpCode.GET_DATA => decodeResponse(reqRecord, buf, GetDataResponse.apply)
        case OpCode.SYNC => decodeResponse(reqRecord, buf, SyncResponse.apply)
        case OpCode.SET_ACL => decodeResponse(reqRecord, buf, SetACLResponse.apply)
        case OpCode.GET_ACL => decodeResponse(reqRecord, buf, GetACLResponse.apply)
        case OpCode.GET_CHILDREN => decodeResponse(reqRecord, buf, GetChildrenResponse.apply)
        case OpCode.GET_CHILDREN2 => decodeResponse(reqRecord, buf, GetChildren2Response.apply)
        case OpCode.SET_WATCHES => decodeHeader(reqRecord, buf)
        case OpCode.MULTI => decodeResponse(reqRecord, buf, TransactionResponse.apply)

        case _ => throw new RuntimeException("RequestRecord was not matched during response reading!")
      }
    }

    def decodeHeader(
      reqRecord: RequestRecord,
      buf: Buf): Future[ResponsePacket] =

      ReplyHeader(buf) match {
        case Return((header, rem)) =>
          checkAssociation(reqRecord, header)
          Future(ResponsePacket(Some(header), None))
        case Throw(exception) => throw exception
      }

    def decodeResponse[T <: Response](
      reqRecord: RequestRecord,
      buf: Buf,
      bodyDecoder: Buf => Try[(T, Buf)]): Future[ResponsePacket] =

      ReplyHeader(buf) match {
        case Return((header, rem)) =>
          try {
            checkAssociation(reqRecord, header)
          } catch {
            case ex: ZkDispatchingException => throw ex
          }

          if (header.err == 0) {
            bodyDecoder(rem) match {
              case Return((body, rem2)) =>
                Future(ResponsePacket(Some(header), Some(body)))
              case Throw(exception) => throw exception
            }
          } else Future(ResponsePacket(Some(header), None))

        case Throw(exception) => throw exception
      }
  }

  class Writer(trans: Transport[Buf, Buf]) {
    def write[Req <: Request](packet: ReqPacket): Future[Unit] =
      if (!hasDispatcherFailed.get()) {
        trans.write(packet.buf) transform {
          case Return(res) => Future(res)
          case Throw(exc) => exc match {
            case exc: Exception
              if exc.isInstanceOf[ChannelException]
                | exc.isInstanceOf[WriteException] =>
              failDispatcher(exc)
              Future.exception(new CancelledRequestException(exc))

            case exc: Exception => Future.exception(new CancelledRequestException(exc))
          }
        }
      } else Future.exception(new CancelledRequestException)
  }

  def failDispatcher(exc: Throwable) {
    // fail incoming requests
    hasDispatcherFailed.set(true)
    // Stop ping
    sessionManager.get.session flatMap { sess =>
      sess.stop()
      sess.currentState.set(States.NOT_CONNECTED)
      Future.Done
    }
    // inform connection manager that the connection is no longer valid
    connectionManager.get.connection flatMap (con => Future(con.isValid.set(false)))
    // fail pending requests
    failPendingRequests(exc)
  }

  def failPendingRequests(exc: Throwable) {
    processedReq.dequeueAll(_ => true).map {
      record =>
        record._2.setException(new CancelledRequestException(exc))
    }
  }
}