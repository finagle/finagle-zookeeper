package com.twitter.finagle.exp.zookeeper.client

import com.twitter.util._
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.Service
import scala.collection.mutable
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.exp.zookeeper.transport._
import org.jboss.netty.buffer.ChannelBuffers._
import com.twitter.finagle.exp.zookeeper._
import com.twitter.util.Throw

trait Dispatcher extends Closable {
  val trans: Transport[ChannelBuffer, ChannelBuffer]
  private[this] val onClose = new Promise[Unit]
  protected var response: Response
  val processedReq = new mutable.SynchronizedQueue[Request]
  val sessionManager = new SessionManager
  val encoder = new Encoder(trans)
  val decoder = new Decoder(trans)

  protected def loop(request: Request) = {
    loopWrite(request) onFailure (_ => close(Time.now))
    loopRead() onFailure (_ => close(Time.now)) onSuccess (response = _)
  }

  private[this] def loopRead(): Future[Response] = {
    doRead(processedReq.front)
  }

  private[this] def loopWrite(request: Request): Future[Unit] = {
    doWrite(request)
  }

  def doWrite(request: Request): Future[Unit] = {
    processedReq.enqueue(request)
    request match {
      case re: ConnectRequest => println("Connect request")
      case re: PingRequest => println("Ping Request")
      case re: CloseSessionRequest => println("Close session request"); sessionManager.stopPing
      case re: CreateRequest =>

    }
    encoder.write(request, sessionManager.getXid)
  }

  def doRead(request: Request): Future[Response] = {
    request match {
      case re: ConnectRequest => println("read connect response"); sessionManager.startPing(encoder.write(new PingRequest, 0))
      case re: PingRequest =>  println("Read ping response")
      case re: CloseSessionRequest => println("Read Close response")
      case re: Request => println("Other type of response")
    }

    trans.read flatMap { buffer =>
      decoder.read(processedReq.dequeue(), buffer)
    }
  }


  override def close(deadline: Time): Future[Unit] = {
    onClose.setDone()
    trans.close(deadline)
  }
}

class ClientDispatcher(val trans: Transport[ChannelBuffer, ChannelBuffer]) extends Service[Request, Response] with Dispatcher {
  override protected var response: Response = new EmptyResponse
  override def apply(request: Request): Future[Response] = {
    loop(request)

    Future(response)
  }
}

class Encoder(trans: Transport[ChannelBuffer, ChannelBuffer]) {
  def write[Req <: Request](request: Req, xid: Int): Future[Unit] = request match {
    case re: ConnectRequest => trans.write(re.toChannelBuffer)
    case re: PingRequest => trans.write(re.toChannelBuffer)
    case re: CloseSessionRequest => trans.write(re.toChannelBuffer)
    case re: Request => trans.write(wrappedBuffer(xidBuffer(xid), re.toChannelBuffer))
  }

  def xidBuffer(xid: Int): ChannelBuffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(0))
    bw.write(-1)
    bw.write(xid)
    bw.underlying.copy()
  }
}

class Decoder(trans: Transport[ChannelBuffer, ChannelBuffer]) {
  def read[Rep >: Response, Req <: Request](request: Req, buffer: ChannelBuffer): Future[Rep] = {
    val br = BufferReader(buffer)
    request match {
      case req: ConnectRequest =>
        val connectRep = ConnectResponse.decode(br)
        Future(connectRep)

      case req: PingRequest =>
        val header = ReplyHeader.decode(br)
        Future.value(header)

      case req: CloseSessionRequest =>
        val header = ReplyHeader.decode(br)
        Future.value(header)

      case req: CreateRequest =>
        val header = ReplyHeader.decode(br)
        val rep = CreateResponse.decode(br)
        Future.value(rep)

      case req: ExistsRequest =>
        val header = ReplyHeader.decode(br)
        Try {ExistsResponse.decode(br)} match {
          case Return(res) =>
            Future.value(res)
          case Throw(ex) => throw ex
        }

      case req: DeleteRequest =>
        val header = ReplyHeader.decode(br)
        Future(new EmptyResponse)

      case req: SetDataRequest =>
        val header = ReplyHeader.decode(br)
        val rep = SetDataResponse.decode(br)
        Future.value(rep)

      case req: GetDataRequest =>
        val header = ReplyHeader.decode(br)
        val rep = GetDataResponse.decode(br)
        Future.value(rep)

      case req: SyncRequest =>
        val header = ReplyHeader.decode(br)
        val rep = SyncResponse.decode(br)
        Future.value(rep)

      case req: SetACLRequest =>
        val header = ReplyHeader.decode(br)
        val rep = SetACLResponse.decode(br)
        Future.value(rep)

      case req: GetACLRequest =>
        val header = ReplyHeader.decode(br)
        val rep = GetACLResponse.decode(br)
        Future.value(rep)

      case req: GetChildrenRequest =>
        val header = ReplyHeader.decode(br)
        val rep = GetChildrenResponse.decode(br)
        Future.value(rep)

      case req: GetChildren2Request =>
        val header = ReplyHeader.decode(br)
        val rep = GetChildren2Response.decode(br)
        Future.value(rep)

      case req: SetWatchesRequest =>
        val header = ReplyHeader.decode(br)
        Future.value(header)

      case req: TransactionRequest =>
        val header = ReplyHeader.decode(br)
        val rep = TransactionResponse.decode(br)
        Future.value(rep)

      case _ => println("THIS IS A NOTIFICATION"); throw new RuntimeException("FUCK NOTIFICATIONS")

    }
  }
}
