package com.twitter.finagle.exp.zookeeper.transport

import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.transport.Transport
import com.twitter.util._
import java.net.SocketAddress
import scala.collection.mutable
import com.twitter.finagle.exp.zookeeper._

case class ZkTransport(
  trans: Transport[ChannelBuffer, ChannelBuffer]
  ) extends Transport[Request, Response] {
  val processedReq = new mutable.SynchronizedQueue[Request]

  /**
   * When receiving a Request
   * @param request the Request to encode
   * @return Future[Unit] when the message is correctly written
   */
  override def write(request: Request): Future[Response] = request match {
    case re: ConnectRequest => doWrite(re)
    case re: CloseSessionRequest => doWrite(re)
    case re: PingRequest => doWrite(re)
    case re: CreateRequest => doWrite(re)
    case re: DeleteRequest => doWrite(re)
    case re: ExistsRequest => doWrite(re)
    case re: GetACLRequest => doWrite(re)
    case re: GetChildrenRequest => doWrite(re)
    case re: GetChildren2Request => doWrite(re)
    case re: GetDataRequest => doWrite(re)
    case re: SetACLRequest => doWrite(re)
    case re: SetDataRequest => doWrite(re)
    case re: SetWatchesRequest => doWrite(re)
    case re: SyncRequest => doWrite(re)
    case re: TransactionRequest => doWrite(re)
  }

  override def read(): Future[Response] = {
    processedReq.front match {
      case rep: ConnectRequest => trans.read flatMap { buffer =>
        val connectRep = ConnectResponse.decode(BufferReader(buffer))
        processedReq.dequeue()
        Future.value(connectRep)
      }
      case re: Request => {
        trans.read flatMap { buffer =>
          decode(re, buffer)
        }
        //Copy the buffer
        //Send the buffer to a decoder
        // If xid == -2/1/-1/-4 Special cases
        // If xid > 1 then common response
        // If decoding was correct (request.xid == rep.xid & no error while decoding)
        //    then dequeue
        // If not put the buffer in a "waiting to decode stack"
      }
      case _ => throw new RuntimeException("READ REQUEST NOT SUPPORTED")
    }
  }

  def decode(request: Request, buffer: ChannelBuffer): Future[Response] = {
    val br = BufferReader(buffer)
    val header = ReplyHeader.decode(br)

    request match {
      case request: CloseSessionRequest if header.xid == 1 =>
        processedReq.dequeue()
        Future.value(header)

      case request: CreateRequest if header.xid > 1=>
        //read zxid
        val rep = CreateResponse.decode(br)
        assert(request.xid == header.xid)
        processedReq.dequeue()
        Future.value(rep)

      case request: ExistsRequest if header.xid > 1=>
        assert(request.xid == header.xid)
        Try {ExistsResponse.decode(br)} match {
          case Return(res) =>
            processedReq.dequeue()
            Future.value(res)
          case Throw(ex) => throw ex
        }

      case request: DeleteRequest if header.xid > 1=>
        assert(request.xid == header.xid)
        processedReq.dequeue()


      case request: SetDataRequest if header.xid > 1=>
        val rep = SetDataResponse.decode(br)
        assert(request.xid == header.xid)
        processedReq.dequeue()
        Future.value(rep)

      case request: GetDataRequest if header.xid > 1=>
        val rep = GetDataResponse.decode(br)
        assert(request.xid == header.xid)
        processedReq.dequeue()
        Future.value(rep)

      case request: PingRequest if header.xid == -2=>
        processedReq.dequeue()
        Future.value(header)

      case request: SyncRequest if header.xid > 1=>
        val rep = SyncResponse.decode(br)
        assert(request.xid == header.xid)
        processedReq.dequeue()
        Future.value(rep)

      case request: SetACLRequest if header.xid > 1=>
        val rep = SetACLResponse.decode(br)
        assert(request.xid == header.xid)
        processedReq.dequeue()
        Future.value(rep)

      case request: GetACLRequest if header.xid > 1=>
        val rep = GetACLResponse.decode(br)
        assert(request.xid == header.xid)
        processedReq.dequeue()
        Future.value(rep)

      case request: GetChildrenRequest if header.xid > 1=>
        val rep = GetChildrenResponse.decode(br)
        assert(request.xid == header.xid)
        processedReq.dequeue()
        Future.value(rep)

      case request: GetChildren2Request if header.xid > 1=>
        val rep = GetChildren2Response.decode(br)
        assert(request.xid == header.xid)
        processedReq.dequeue()
        Future.value(rep)

      case request: SetWatchesRequest if header.xid > 1=>
        assert(request.xid == header.xid)
        processedReq.dequeue()
        Future.value(header)

      case request: TransactionRequest if header.xid > 1=>
        val rep = TransactionResponse.decode(br)
        assert(request.xid == header.xid)
        processedReq.dequeue()
        Future.value(rep)

      case _ if header.xid == -1=> println("THIS IS A NOTIFICATION"); throw new RuntimeException("FUCK")
    }
  }

  def doWrite(request: Request): Future[Response] = {
    processedReq.enqueue(request)
    trans.write(request.toChannelBuffer)
  }

  override def remoteAddress: SocketAddress = trans.remoteAddress
  override def localAddress: SocketAddress = trans.localAddress
  override def isOpen: Boolean = trans.isOpen
  override val onClose: Future[Throwable] = trans.onClose
  override def close(deadline: Time): Future[Unit] = trans.close(deadline)
}
