package com.twitter.finagle.exp.zookeeper.client

import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.exp.zookeeper._
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.exp.zookeeper.{Response, Request}
import com.twitter.util._
import com.twitter.finagle.exp.zookeeper.transport.BufferReader
import com.twitter.finagle.exp.zookeeper.ZookeeperDefinitions.opCode
import scala.collection.mutable
import com.twitter.util.Throw
import com.twitter.finagle.exp.zookeeper.watch.{WatchType, WatchManager}

class RequestMatcher(trans: Transport[ChannelBuffer, ChannelBuffer], sender: Request => Future[Response]) {
  val sessionManager = new SessionManager(sender, true)
  val processedReq = new mutable.SynchronizedQueue[RequestRecord]
  val watchManager = new WatchManager()

  val encoder = new Writer(trans)
  val decoder = new Reader(trans)

  def read(): Future[Response] = {
    val fullRep = trans.read() flatMap { buffer =>
      if (processedReq.size > 0)
        decoder.read(Some(processedReq.front), buffer)
      else decoder.read(None, buffer)
    } onFailure { exc =>
      // check if we dequeue the correct request
      println(exc.getMessage)
      processedReq.dequeue()
      Future.exception(exc)
    }

    fullRep flatMap { pureRep =>
      sessionManager(pureRep._1)
      pureRep._2 match {
        case rep: ConnectResponse =>
          sessionManager.completeConnection(rep)
          println("---> CONNECT | timeout: " +
            rep.timeOut + " | sessionID: " +
            rep.sessionId + " | canRO: " +
            rep.canRO.getOrElse(false))
          processedReq.dequeue()
          Future(rep)
        case rep: CreateResponse =>
          println("---> CREATE")
          processedReq.dequeue()
          Future(rep)
        case rep: ExistsResponse =>
          println("---> EXISTS")
          processedReq.dequeue()
          Future(rep)
        case rep: SetDataResponse =>
          println("---> SETDATA")
          processedReq.dequeue()
          Future(rep)
        case rep: GetDataResponse =>
          println("---> GETDATA")
          processedReq.dequeue()
          Future(rep)
        case rep: SyncResponse =>
          println("---> SYNC")
          processedReq.dequeue()
          Future(rep)
        case rep: SetACLResponse =>
          println("---> SETACL")
          processedReq.dequeue()
          Future(rep)
        case rep: GetACLResponse =>
          println("---> GETACL")
          processedReq.dequeue()
          Future(rep)
        case rep: GetChildrenResponse =>
          println("---> GETCHILDREN")
          processedReq.dequeue()
          Future(rep)
        case rep: GetChildren2Response =>
          println("---> GETCHILDREN2")
          processedReq.dequeue()
          Future(rep)
        case rep: TransactionResponse =>
          println("---> TRANSACTION")
          processedReq.dequeue()
          Future(rep)
        case rep: EmptyResponse =>
          println("---> Empty Response")
          processedReq.dequeue()
          Future(rep)
        case rep: WatchEvent =>
          println("---> Watches event")
          watchManager.process(rep)
          sessionManager(rep)
          read()
      }
    }
  }

  def write(req: Request): Future[Unit] = {
    val packet = req match {
      case req: ConnectRequest =>
        println("<--- CONNECT")
        sessionManager.checkStateBeforeConnect
        sessionManager.prepareConnection
        Packet(None, Some(req))

      case req: PingRequest =>
        println("<--- PING")
        sessionManager.checkState
        Packet(Some(req), None)

      case req: CloseSessionRequest =>
        sessionManager.checkState
        sessionManager.prepareCloseSession
        println("<--- CLOSE SESSION")
        Packet(Some(req), None)

      case req: CreateRequest =>
        println("<--- CREATE")
        sessionManager.checkState
        Packet(Some(RequestHeader(sessionManager.getXid, opCode.create)), Some(req))

      case req: ExistsRequest =>
        println("<--- EXISTS")
        sessionManager.checkState
        val xid = sessionManager.getXid
        if (req.watch) watchManager.prepareRegister(req.path, WatchType.exists, xid)
        Packet(Some(RequestHeader(xid, opCode.exists)), Some(req))

      case req: DeleteRequest =>
        println("<--- DELETE")
        sessionManager.checkState
        Packet(Some(RequestHeader(sessionManager.getXid, opCode.delete)), Some(req))

      case req: SetDataRequest =>
        println("<--- SETDATA")
        sessionManager.checkState
        Packet(Some(RequestHeader(sessionManager.getXid, opCode.setData)), Some(req))

      case req: GetDataRequest =>
        println("<--- GETDATA")
        sessionManager.checkState
        val xid = sessionManager.getXid
        if (req.watch) watchManager.prepareRegister(req.path, WatchType.data, xid)
        Packet(Some(RequestHeader(xid, opCode.getData)), Some(req))

      case req: SyncRequest =>
        println("<--- SYNC")
        sessionManager.checkState
        Packet(Some(RequestHeader(sessionManager.getXid, opCode.sync)), Some(req))

      case req: SetACLRequest =>
        println("<--- SETACL")
        sessionManager.checkState
        Packet(Some(RequestHeader(sessionManager.getXid, opCode.setACL)), Some(req))

      case req: GetACLRequest =>
        println("<--- GETACL")
        sessionManager.checkState
        Packet(Some(RequestHeader(sessionManager.getXid, opCode.getACL)), Some(req))

      case req: GetChildrenRequest =>
        println("<--- GETCHILDREN")
        sessionManager.checkState
        val xid = sessionManager.getXid
        if (req.watch) watchManager.prepareRegister(req.path, WatchType.child, xid)
        Packet(Some(RequestHeader(xid, opCode.getChildren)), Some(req))

      case req: GetChildren2Request =>
        println("<--- GETCHILDREN2")
        sessionManager.checkState
        val xid = sessionManager.getXid
        if (req.watch) watchManager.prepareRegister(req.path, WatchType.child, xid)
        Packet(Some(RequestHeader(xid, opCode.getChildren2)), Some(req))

      case req: SetWatchesRequest =>
        println("<--- SETWATCHES")
        sessionManager.checkState
        Packet(Some(RequestHeader(sessionManager.getXid, opCode.setWatches)), Some(req))

      case req: TransactionRequest =>
        println("<--- TRANSACTION")
        sessionManager.checkState
        Packet(Some(RequestHeader(sessionManager.getXid, opCode.multi)), Some(req))

      case _ => throw new RuntimeException("Request type not supported")
    }

    val reqRecord = packet match {
      case Packet(Some(header), _) => RequestRecord(header.opCode, Some(header.xid))
      case Packet(None, _) => RequestRecord(opCode.createSession, None)
    }

    processedReq.enqueue(reqRecord)
    encoder.write(packet)
  }

  def checkAssociation(reqRecord: RequestRecord, repHeader: ReplyHeader) = {
    println("--- associating:  %d and %d ---".format(reqRecord.xid.get, repHeader.xid))
    if (reqRecord.xid.isDefined)
      if (reqRecord.xid.get != repHeader.xid) throw new RuntimeException("wrong association")

  }

  def checkAssociationResult(reqRecord: RequestRecord, response: Response) = {
    response match {
      case rep: EmptyResponse =>
        assert(reqRecord.opCode == opCode.ping || reqRecord.opCode == opCode.delete
          || reqRecord.opCode == opCode.setWatches || reqRecord.opCode == opCode.closeSession)
      case rep: ConnectResponse =>
        assert(reqRecord.opCode == opCode.createSession)
      case rep: CreateResponse =>
        assert(reqRecord.opCode == opCode.create)
      case rep: ExistsResponse =>
        assert(reqRecord.opCode == opCode.exists)
      case rep: SetDataResponse =>
        assert(reqRecord.opCode == opCode.setData)
      case rep: GetDataResponse =>
        assert(reqRecord.opCode == opCode.getData)
      case rep: SyncResponse =>
        assert(reqRecord.opCode == opCode.sync)
      case rep: SetACLResponse =>
        assert(reqRecord.opCode == opCode.setACL)
      case rep: GetACLResponse =>
        assert(reqRecord.opCode == opCode.getACL)
      case rep: GetChildrenResponse =>
        assert(reqRecord.opCode == opCode.getChildren)
      case rep: GetChildren2Response =>
        assert(reqRecord.opCode == opCode.getChildren2)
      case rep: TransactionResponse =>
        assert(reqRecord.opCode == opCode.multi)
    }
  }


  class Reader(trans: Transport[ChannelBuffer, ChannelBuffer]) {

    def read(reqRecord: Option[RequestRecord], buffer: ChannelBuffer): Future[(ReplyHeader, Response)] = {
      val br = BufferReader(buffer)

      println(processedReq.toList)
      val rep = reqRecord match {
        case Some(record) => readFromRequest(record, br)
        case None => readFromHeader(br)
      }

      rep match {
        case Return(response) => response
        case Throw(exc1) =>
          //Two possibilities : zookeeper exception or decoding error
          if (exc1.isInstanceOf[ZookeeperException]) {
            Future.exception(exc1)
          }
          else {
            br.underlying.resetReaderIndex()
            readFromHeader(br) match {
              case Return(eventRep) => eventRep
              case Throw(exc2) => throw new RuntimeException("Impossible to decode response")
            }
          }
      }
    }

    def readFromHeader(br: BufferReader): Try[Future[(ReplyHeader, Response)]] = Try {
      val xid = br.readInt
      val zxid = br.readLong
      val err = br.readInt

      if (err == 0) {
        xid match {
          case -2 =>
            Future(new ReplyHeader(xid, zxid, err), new EmptyResponse)
          case -1 =>
            Future(new ReplyHeader(xid, zxid, err), WatchEvent.decode(br))
          case 1 =>
            Future(new ReplyHeader(xid, zxid, err), new EmptyResponse)
          case -4 =>
            Future(new ReplyHeader(xid, zxid, err), new EmptyResponse)
        }
      } else {
        throw ZookeeperException.create("Error while readFromHeader :", err)
      }
    }

    def readFromRequest(reqRecord: RequestRecord, br: BufferReader): Try[Future[(ReplyHeader, Response)]] = Try {
      val rep = reqRecord.opCode match {
        case opCode.createSession =>
          ConnectResponse(br) match {
            case Return(header) =>
              //correct here
              Future(new ReplyHeader(0, 0, -666), header)
            case Throw(exception) =>
              sessionManager.connectionFailed
              throw exception
          }

        case opCode.ping =>
          ReplyHeader(br) match {
            case Return(header) =>
              checkAssociation(reqRecord, header)
              Future(header, new EmptyResponse)
            case Throw(exception) => throw exception
          }

        case opCode.closeSession =>
          ReplyHeader(br) match {
            case Return(header) =>
              checkAssociation(reqRecord, header)
              sessionManager.completeCloseSession
              Future(header, new EmptyResponse)
            case Throw(exception) => throw exception
          }

        case opCode.create =>
          ReplyHeader(br) match {
            case Return(header) =>
              checkAssociation(reqRecord, header)
              CreateResponse(br) match {
                case Return(body) => Future(header, body)
                case Throw(exception) => throw exception
              }
            case Throw(exception) => throw exception
          }

        case opCode.exists =>
          ReplyHeader(br) match {
            case Return(header) =>
              checkAssociation(reqRecord, header)
              val watch = watchManager.register(header.xid)
              ExistsResponse.decodeWithWatch(br, watch) match {
                case Return(body) => Future(header, body)
                case Throw(exception) => throw exception
              }
            case Throw(exception) => throw exception
          }

        case opCode.delete =>
          ReplyHeader(br) match {
            case Return(header) =>
              checkAssociation(reqRecord, header)
              Future(header, new EmptyResponse)
            case Throw(exception) => throw exception
          }

        case opCode.setData =>
          ReplyHeader(br) match {
            case Return(header) =>
              checkAssociation(reqRecord, header)
              SetDataResponse(br) match {
                case Return(body) => Future(header, body)
                case Throw(exception) => throw exception
              }
            case Throw(exception) => throw exception
          }

        case opCode.getData =>
          ReplyHeader(br) match {
            case Return(header) =>
              checkAssociation(reqRecord, header)
              val watch = watchManager.register(header.xid)
              GetDataResponse.decodeWithWatch(br, watch) match {
                case Return(body) => Future(header, body)
                case Throw(exception) => throw exception
              }
            case Throw(exception) => throw exception
          }

        case opCode.sync =>
          ReplyHeader(br) match {
            case Return(header) =>
              checkAssociation(reqRecord, header)
              SyncResponse(br) match {
                case Return(body) => Future(header, body)
                case Throw(exception) => throw exception
              }
            case Throw(exception) => throw exception
          }

        case opCode.setACL =>
          ReplyHeader(br) match {
            case Return(header) =>
              checkAssociation(reqRecord, header)
              SetACLResponse(br) match {
                case Return(body) => Future(header, body)
                case Throw(exception) => throw exception
              }
            case Throw(exception) => throw exception
          }

        case opCode.getACL =>
          ReplyHeader(br) match {
            case Return(header) =>
              checkAssociation(reqRecord, header)
              GetACLResponse(br) match {
                case Return(body) => Future(header, body)
                case Throw(exception) => throw exception
              }
            case Throw(exception) => throw exception
          }

        case opCode.getChildren =>
          ReplyHeader(br) match {
            case Return(header) =>
              checkAssociation(reqRecord, header)
              val watch = watchManager.register(header.xid)
              GetChildrenResponse.decodeWithWatch(br, watch) match {
                case Return(body) => Future(header, body)
                case Throw(exception) => throw exception
              }
            case Throw(exception) => throw exception
          }

        case opCode.getChildren2 =>
          ReplyHeader(br) match {
            case Return(header) =>
              checkAssociation(reqRecord, header)
              val watch = watchManager.register(header.xid)
              GetChildren2Response.decodeWithWatch(br, watch) match {
                case Return(body) => Future(header, body)
                case Throw(exception) => throw exception
              }
            case Throw(exception) => throw exception
          }

        case opCode.setWatches =>
          ReplyHeader(br) match {
            case Return(header) =>
              checkAssociation(reqRecord, header)
              Future(header, new EmptyResponse)
            case Throw(exception) => throw exception
          }

        case opCode.multi =>
          ReplyHeader(br) match {
            case Return(header) =>
              checkAssociation(reqRecord, header)
              TransactionResponse(br) match {
                case Return(body) => Future(header, body)
                case Throw(exception) => throw exception
              }
            case Throw(exception) => throw exception
          }

        case _ => println("Not POSSIBLE"); throw new RuntimeException("MATCH RESPONSE ERROR")
      }

      rep flatMap { response =>
        //Check if we decoded the right response
        checkAssociationResult(reqRecord, response._2)
        Future(response)
      }
    }
  }

  class Writer(trans: Transport[ChannelBuffer, ChannelBuffer]) {
    def write[Req <: Request](packet: Packet): Future[Unit] = trans.write(packet.serialize)
  }
}

