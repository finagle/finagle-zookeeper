package com.twitter.finagle.exp.zookeeper.client

import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.exp.zookeeper._
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.exp.zookeeper.{Response, Request}
import com.twitter.util._
import com.twitter.finagle.exp.zookeeper.transport.BufferReader
import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.OpCode
import scala.collection.mutable
import com.twitter.util.Throw
import com.twitter.finagle.exp.zookeeper.watch.{WatchType, WatchManager}
import java.util.concurrent.atomic.AtomicBoolean

class RequestMatcher(
  trans: Transport[ChannelBuffer, ChannelBuffer],
  sender: Request => Future[Response]) {
  /**
   * Local variables
   * processesReq - queue of requests waiting for responses
   * watchManager - the session watch manager
   * sessionManager - the session manager
   * isReading - the reading loop is started or not
   *
   * encoder - creates a complete Packet from a request
   * decoder - creates a response from a Buffer
   */
  val processedReq = new mutable.SynchronizedQueue[(RequestRecord, Promise[Response])]
  val watchManager = new WatchManager()
  val sessionManager = new SessionManager(sender, watchManager, true)
  val isReading = new AtomicBoolean(false)

  val encoder = new Writer(trans)
  val decoder = new Reader(trans)

  def read(): Future[Unit] = {
    val fullRep = trans.read() flatMap { buffer =>
      val currentReq: Option[(RequestRecord, Promise[Response])] =
        if (processedReq.size > 0) Some(processedReq.front) else None

      decoder.read(currentReq, buffer) onFailure { exc =>
        // If this exception is associated to a request, then propagate in the promise
        if (currentReq.isDefined) {
          processedReq.dequeue()._2.setException(exc)
        } else {
          throw new RuntimeException("Undefined exception during reading ", exc)
        }
      }
    }

    fullRep transform {
      case Return(rep) => rep match {
        case rep: WatchEvent =>
          // Notifies the watch manager we have a new watchEvent
          watchManager.process(rep)
          // Notifies the session manager we have a new watchEvent
          sessionManager(rep)
          // Continue to read if a response if expected
          Future.Done
        case rep: Response =>
          // The request record is dequeued now that it's satisfied
          processedReq.dequeue()._2.setValue(rep)
          Future.Done
      }

      // This case is already handled during decoder.read (see onFailure)
      case Throw(exc) =>
        Future.Done
    }
  }

  def readLoop(): Future[Unit] = {
    if (!sessionManager.isClosing) read() before readLoop()
    else Future.Done
  }

  /**
   * We make decisions depending on request type, a Packet is created,
   * a request record of the Packet is added to the queue, the packet is
   * finally written to the transport.
   * @param req the request to send
   * @return a Future[Unit] when the request is completely written
   */
  def write(req: Request): Future[Response] = {
    val packet = req match {
      case req: AuthRequest =>
        println("<--- AUTH")
        // Check the connection state before writing
        sessionManager.checkState()
        Packet(Some(RequestHeader(-4, OpCode.AUTH)), Some(req))

      case req: ConnectRequest =>
        println("<--- CONNECT")
        sessionManager.checkStateBeforeConnect()
        // Notifies the session manager that a new connection is being created
        sessionManager.prepareConnection()
        Packet(None, Some(req))

      case req: PingRequest =>
        println("<--- PING")
        sessionManager.checkState()
        Packet(Some(req), None)

      case req: CloseSessionRequest =>
        sessionManager.checkState()
        // Notifies the session manager that the connection is being closed
        sessionManager.prepareCloseSession()
        println("<--- CLOSE SESSION")
        Packet(Some(req), None)

      case req: CreateRequest =>
        println("<--- CREATE")
        sessionManager.checkState()
        Packet(Some(RequestHeader(sessionManager.getXid, OpCode.CREATE)), Some(req))

      case req: ExistsRequest =>
        println("<--- EXISTS")
        sessionManager.checkState()
        val xid = sessionManager.getXid
        // Notifies the watch manager that we are about to create a new watch
        if (req.watch) watchManager.prepareRegister(req.path, WatchType.exists, xid)
        Packet(Some(RequestHeader(xid, OpCode.EXISTS)), Some(req))

      case req: DeleteRequest =>
        println("<--- DELETE")
        sessionManager.checkState()
        Packet(Some(RequestHeader(sessionManager.getXid, OpCode.DELETE)), Some(req))

      case req: SetDataRequest =>
        println("<--- SETDATA")
        sessionManager.checkState()
        Packet(Some(RequestHeader(sessionManager.getXid, OpCode.SET_DATA)), Some(req))

      case req: GetDataRequest =>
        println("<--- GETDATA")
        sessionManager.checkState()
        val xid = sessionManager.getXid
        // Notifies the watch manager that we are about to create a new watch
        if (req.watch) watchManager.prepareRegister(req.path, WatchType.data, xid)
        Packet(Some(RequestHeader(xid, OpCode.GET_DATA)), Some(req))

      case req: SyncRequest =>
        println("<--- SYNC")
        sessionManager.checkState()
        Packet(Some(RequestHeader(sessionManager.getXid, OpCode.SYNC)), Some(req))

      case req: SetACLRequest =>
        println("<--- SETACL")
        sessionManager.checkState()
        Packet(Some(RequestHeader(sessionManager.getXid, OpCode.SET_ACL)), Some(req))

      case req: GetACLRequest =>
        println("<--- GETACL")
        sessionManager.checkState()
        Packet(Some(RequestHeader(sessionManager.getXid, OpCode.GET_ACL)), Some(req))

      case req: GetChildrenRequest =>
        println("<--- GETCHILDREN")
        sessionManager.checkState()
        val xid = sessionManager.getXid
        // Notifies the watch manager that we are about to create a new watch
        if (req.watch) watchManager.prepareRegister(req.path, WatchType.child, xid)
        Packet(Some(RequestHeader(xid, OpCode.GET_CHILDREN)), Some(req))

      case req: GetChildren2Request =>
        println("<--- GETCHILDREN2")
        sessionManager.checkState()
        val xid = sessionManager.getXid
        // Notifies the watch manager that we are about to create a new watch
        if (req.watch) watchManager.prepareRegister(req.path, WatchType.child, xid)
        Packet(Some(RequestHeader(xid, OpCode.GET_CHILDREN2)), Some(req))

      case req: SetWatchesRequest =>
        println("<--- SETWATCHES")
        sessionManager.checkState()
        Packet(Some(RequestHeader(-8, OpCode.SET_WATCHES)), Some(req))

      case req: TransactionRequest =>
        println("<--- TRANSACTION")
        sessionManager.checkState()
        Packet(Some(RequestHeader(sessionManager.getXid, OpCode.MULTI)), Some(req))

      case _ => throw new RuntimeException("Request type not supported")
    }

    val reqRecord = packet match {
      case Packet(Some(header), _) => RequestRecord(header.opCode, Some(header.xid))
      case Packet(None, _) => RequestRecord(OpCode.CREATE_SESSION, None)
    }

    // The request is about to be written, so we enqueue it in the waiting request queue
    val p = new Promise[Response]()

    synchronized {
      processedReq.enqueue((reqRecord, p))
      encoder.write(packet) flatMap { unit =>
        if (!isReading.getAndSet(true)) {
          readLoop()
        }
        p
      }
    }
  }

  /**
   * Checks an association between a request and a ReplyHeader
   * @param reqRecord expected request details
   * @param repHeader freshly decoded ReplyHeader
   */
  def checkAssociation(reqRecord: RequestRecord, repHeader: ReplyHeader) = {
    println("--- associating:  %d and %d ---".format(reqRecord.xid.get, repHeader.xid))
    if (reqRecord.xid.isDefined)
      if (reqRecord.xid.get != repHeader.xid) throw new RuntimeException("wrong association")

  }

  class Reader(trans: Transport[ChannelBuffer, ChannelBuffer]) {

    /**
     * This function will try to decode the buffer depending if a request
     * is waiting for a response or not.
     * In case there is a request, it will try to decode with readFromRequest,
     * if an exception is thrown then it will try again with readFromHeader.
     * @param req expected request description
     * @param buffer current buffer
     * @return
     */
    def read(
      req: Option[(RequestRecord, Promise[Response])],
      buffer: ChannelBuffer): Future[Response] = {
      val bufferReader = BufferReader(buffer)

      /**
       * if Some(record) then a request is waiting for a response
       * if None then this is a watchEvent
       */
      val rep = req match {
        case Some(record) =>
          println("Read from request")
          readFromRequest(record, bufferReader)
        case None =>
          println("Read from header")
          readFromHeader(bufferReader)
      }

      rep match {
        case Return(response) => response

        case Throw(exc1) =>
          //Two possibilities : zookeeper exception or decoding error
          if (exc1.isInstanceOf[ZookeeperException]) {
            println("THROW")
            Future.exception(exc1)
          }
          else {
            /**
             * This is a decoding error, we should try to decode the buffer
             * as a watchEvent
             */
            bufferReader.underlying.resetReaderIndex()
            readFromHeader(bufferReader) match {
              case Return(eventRep) => eventRep
              case Throw(exc2) =>
                throw new RuntimeException("Impossible to decode this response", exc1)
            }
          }
      }
    }

    /**
     * We try to decode the buffer by reading the ReplyHeader
     * and matching the xid to find the message's type
     * @param br the current buffer reader
     * @return possibly the (header, response) or an exception
     */
    def readFromHeader(br: BufferReader): Try[Future[Response]] = Try {
      val header = ReplyHeader(br) match {
        case Return(res) => res
        case Throw(exc) => throw exc
      }

      if (header.err == 0) {
        header.xid match {
          case -2 =>
            Future(new EmptyResponse)
          case -1 =>
            println("---> Watches event")

            WatchEvent(br) match {
              case Return(event) => Future(event)
              case Throw(exc) =>
                sessionManager(header)
                Future.exception(exc)
            }

          case 1 =>
            Future(new EmptyResponse)
          case -4 =>
            Future(new EmptyResponse)
        }
      } else {
        sessionManager(header)
        Future.exception(ZookeeperException.create("Error while readFromHeader :", header.err))
      }
    }

    /**
     * If a request is expected, this function will be called first
     * the expected response opCode is matched, so that we have the
     * correct way to decode. The header is decoded first, the resulting
     * header is checked with checkAssociation to make sure both xids
     * are equal.
     * The body is decoded next and sent with the header in a Future
     *
     * If any exception is thrown then the readFromHeader will be called
     * with the rewinded buffer reader
     * @param req expected request
     * @param br current buffer reader
     * @return possibly the (header, response) or an exception
     */
    def readFromRequest(
      req: (RequestRecord, Promise[Response]),
      br: BufferReader): Try[Future[Response]] = Try {

      req._1.opCode match {
        case OpCode.AUTH =>
          ReplyHeader(br) match {
            case Return(header) =>
              checkAssociation(req._1, header)
              sessionManager(header)

              if (header.err == 0) {
                println("---> AUTH")
                Future(new EmptyResponse)
              } else {
                throw ZookeeperException.create("Error while auth", header.err)
              }

            case Throw(exc) => throw exc
          }

        case OpCode.CREATE_SESSION =>
          ConnectResponse(br) match {
            case Return(body) =>
              println("---> CONNECT | timeout: " +
                body.timeOut + " | sessionID: " +
                body.sessionId + " | canRO: " +
                body.canRO.getOrElse(false))
              // Notifies the session manager the connection is established
              sessionManager.completeConnection(body)

              Future(body)

            case Throw(exception) =>
              // Notifies the session manager the connection has failed
              sessionManager.connectionFailed()
              throw exception
          }

        case OpCode.PING =>
          ReplyHeader(br) match {
            case Return(header) =>
              checkAssociation(req._1, header)
              sessionManager(header)

              if (header.err == 0) {
                println("---> PING")
                Future(new EmptyResponse)
              } else {
                throw ZookeeperException.create("Error while ping", header.err)
              }

            case Throw(exception) => throw exception
          }

        case OpCode.CLOSE_SESSION =>
          ReplyHeader(br) match {
            case Return(header) =>
              checkAssociation(req._1, header)
              sessionManager(header)

              if (header.err == 0) {
                // Notifies the session manager the connection has successfully closed
                sessionManager.completeCloseSession()
                println("---> CLOSE SESSION")
                Future(new EmptyResponse)
              } else {
                throw ZookeeperException.create("Error while close session", header.err)
              }

            case Throw(exception) => throw exception
          }

        case OpCode.CREATE =>
          ReplyHeader(br) match {
            case Return(header) =>
              checkAssociation(req._1, header)
              sessionManager(header)

              if (header.err == 0) {
                CreateResponse(br) match {
                  case Return(body) =>
                    println("---> CREATE")
                    Future(body)
                  case Throw(exception) => throw exception
                }
              } else {
                throw ZookeeperException.create("Error while create :", header.err)
              }

            case Throw(exception) => throw exception
          }

        case OpCode.EXISTS =>
          ReplyHeader(br) match {
            case Return(header) =>
              checkAssociation(req._1, header)

              if (header.err == 0 || header.err == -101) {
                // Notifies the watch manager that the watch has successfully registered
                sessionManager(header)
                watchManager.register(header.xid) match {
                  case Some(watch) =>
                    ExistsResponse.decodeWithWatch(br, Some(watch), header) match {
                      case Return(body) =>
                        println("---> EXISTS")
                        Future(body)
                      case Throw(exception) => throw exception
                    }
                  case None =>
                    if (header.err == 0) {
                      ExistsResponse(br) match {
                        case Return(body) =>
                          println("---> EXISTS")
                          Future(body)
                        case Throw(exception) => throw exception
                      }
                    } else throw ZookeeperException.create("Error while exists", header.err)
                }

              } else {
                throw ZookeeperException.create("Error while exists", header.err)
              }

            case Throw(exception) => throw exception
          }

        case OpCode.DELETE =>
          ReplyHeader(br) match {
            case Return(header) =>
              checkAssociation(req._1, header)
              sessionManager(header)

              if (header.err == 0) {
                println("---> DELETE")
                Future(new EmptyResponse)
              } else {
                throw ZookeeperException.create("Error while delete", header.err)
              }

            case Throw(exception) => throw exception
          }

        case OpCode.SET_DATA =>
          ReplyHeader(br) match {
            case Return(header) =>
              checkAssociation(req._1, header)
              sessionManager(header)

              if (header.err == 0) {
                SetDataResponse(br) match {
                  case Return(body) =>
                    println("---> SETDATA")
                    Future(body)
                  case Throw(exception) => throw exception
                }
              } else {
                throw ZookeeperException.create("Error while setData", header.err)
              }

            case Throw(exception) => throw exception
          }

        case OpCode.GET_DATA =>
          ReplyHeader(br) match {
            case Return(header) =>
              checkAssociation(req._1, header)

              if (header.err == 0) {
                // Notifies the watch manager that the watch has successfully registered
                sessionManager(header)
                val watch = watchManager.register(header.xid)
                GetDataResponse.decodeWithWatch(br, watch) match {
                  case Return(body) =>
                    println("---> GETDATA")
                    Future(body)
                  case Throw(exception) => throw exception
                }
              } else if (header.err == -101) {
                watchManager.cancelPrepare(header.xid)
                throw ZookeeperException.create("Error while getData", header.err)
              } else {
                throw ZookeeperException.create("Error while getData", header.err)
              }

            case Throw(exception) => throw exception
          }

        case OpCode.SYNC =>
          ReplyHeader(br) match {
            case Return(header) =>
              checkAssociation(req._1, header)
              sessionManager(header)

              if (header.err == 0) {
                SyncResponse(br) match {
                  case Return(body) =>
                    println("---> SYNC")
                    Future(body)
                  case Throw(exception) => throw exception
                }
              } else {
                throw ZookeeperException.create("Error while sync", header.err)
              }

            case Throw(exception) => throw exception
          }

        case OpCode.SET_ACL =>
          ReplyHeader(br) match {
            case Return(header) =>
              checkAssociation(req._1, header)
              sessionManager(header)

              if (header.err == 0) {
                SetACLResponse(br) match {
                  case Return(body) =>
                    println("---> SETACL")
                    Future(body)
                  case Throw(exception) => throw exception
                }
              } else {
                throw ZookeeperException.create("Error while setACL", header.err)
              }

            case Throw(exception) => throw exception
          }

        case OpCode.GET_ACL =>
          ReplyHeader(br) match {
            case Return(header) =>
              checkAssociation(req._1, header)
              sessionManager(header)

              if (header.err == 0) {
                GetACLResponse(br) match {
                  case Return(body) =>
                    println("---> GET_ACL")
                    Future(body)
                  case Throw(exception) => throw exception
                }
              } else {
                throw ZookeeperException.create("Error while getACL", header.err)
              }

            case Throw(exception) => throw exception
          }

        case OpCode.GET_CHILDREN =>
          ReplyHeader(br) match {
            case Return(header) =>
              checkAssociation(req._1, header)
              sessionManager(header)

              if (header.err == 0) {
                // Notifies the watch manager that the watch has successfully registered
                val watch = watchManager.register(header.xid)
                GetChildrenResponse.decodeWithWatch(br, watch) match {
                  case Return(body) =>
                    println("---> GET_CHILDREN")
                    Future(body)
                  case Throw(exception) => throw exception
                }
              } else if (header.err == -101) {
                watchManager.cancelPrepare(header.xid)
                throw ZookeeperException.create("Error while getChildren", header.err)
              } else {
                throw ZookeeperException.create("Error while getChildren", header.err)
              }

            case Throw(exception) => throw exception
          }

        case OpCode.GET_CHILDREN2 =>
          ReplyHeader(br) match {
            case Return(header) =>
              checkAssociation(req._1, header)
              sessionManager(header)

              if (header.err == 0) {
                // Notifies the watch manager that the watch has successfully registered
                val watch = watchManager.register(header.xid)
                GetChildren2Response.decodeWithWatch(br, watch) match {
                  case Return(body) =>
                    println("---> GET_CHILDREN2")
                    Future(body)
                  case Throw(exception) => throw exception
                }
              } else if (header.err == -101) {
                watchManager.cancelPrepare(header.xid)
                throw ZookeeperException.create("Error while getChildren2", header.err)
              } else {
                throw ZookeeperException.create("Error while getChildren2", header.err)
              }

            case Throw(exception) => throw exception
          }

        case OpCode.SET_WATCHES =>
          ReplyHeader(br) match {
            case Return(header) =>
              checkAssociation(req._1, header)
              sessionManager(header)

              if (header.err == 0) {
                println("---> SET_WATCHES")
                Future(new EmptyResponse)
              } else {
                throw ZookeeperException.create("Error while setWatches", header.err)
              }

            case Throw(exception) => throw exception
          }

        case OpCode.MULTI =>
          ReplyHeader(br) match {
            case Return(header) =>
              checkAssociation(req._1, header)
              sessionManager(header)

              if (header.err == 0) {
                TransactionResponse(br) match {
                  case Return(body) =>
                    println("---> TRANSACTION")
                    Future(body)
                  case Throw(exception) => throw exception
                }
              } else {
                // TODO return partial result
                throw ZookeeperException.create("Error while transaction", header.err)
              }

            case Throw(exception) => throw exception
          }

        case _ => throw new RuntimeException("RequestRecord was not matched during response reading!")
      }
    }
  }

  class Writer(trans: Transport[ChannelBuffer, ChannelBuffer]) {
    def write[Req <: Request](packet: Packet): Future[Unit] = trans.write(packet.serialize)
  }
}