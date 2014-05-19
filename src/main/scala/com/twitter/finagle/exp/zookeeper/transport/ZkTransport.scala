package com.twitter.finagle.exp.zookeeper.transport

import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.exp.zookeeper._
import com.twitter.util._
import java.net.SocketAddress
import com.twitter.finagle.exp.zookeeper.ConnectRequest
import scala.collection.mutable
import com.twitter.finagle.exp.zookeeper.GetChildrenRequest
import com.twitter.finagle.exp.zookeeper.GetDataRequest
import com.twitter.finagle.exp.zookeeper.SetWatchesRequest
import com.twitter.finagle.exp.zookeeper.SyncRequest
import com.twitter.finagle.exp.zookeeper.GetACLRequest
import com.twitter.finagle.exp.zookeeper.TransactionRequest
import com.twitter.finagle.exp.zookeeper.SetACLRequest
import com.twitter.finagle.exp.zookeeper.CreateRequest
import com.twitter.finagle.exp.zookeeper.RequestHeader
import com.twitter.finagle.exp.zookeeper.DeleteRequest
import com.twitter.finagle.exp.zookeeper.ExistsRequest
import com.twitter.finagle.exp.zookeeper.ConnectRequest
import com.twitter.finagle.exp.zookeeper.GetChildren2Request
import com.twitter.finagle.exp.zookeeper.SetDataRequest

case class ZkTransport(
  trans: Transport[ChannelBuffer, ChannelBuffer]
  ) extends Transport[Request, Response] {
  val processedReq = new mutable.SynchronizedQueue[Request]

  /**
   * When receiving a Request
   * @param req the Request to encode
   * @return Future[Unit] when the message is correctly written
   */
  override def write(req: Request): Future[Unit] = req match {
    case re: ConnectRequest => doWrite(re)
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
    case re: RequestHeader => doWrite(re)
  }

  override def read(): Future[Response] = {
    processedReq.front match {
      case rep: ConnectRequest => trans.read flatMap { buffer =>
        val connectRep = ConnectResponse.decode(Buffer.fromChannelBuffer(buffer))
        processedReq.dequeue()
        Future.value(connectRep)
      }
      case rep: RequestHeader => trans.read flatMap { buffer =>
        val replyRep = ReplyHeader.decode(Buffer.fromChannelBuffer(buffer))
        processedReq.dequeue()
        Future.value(replyRep)
      }
      case rep:CreateRequest => trans.read flatMap {buffer =>
        val rep = CreateResponse.decode(Buffer.fromChannelBuffer(buffer))
        processedReq.dequeue()
        Future.value(rep)
      }
      case rep:ExistsRequest => trans.read flatMap {buffer =>
        processedReq.dequeue()
        Try {ExistsResponse.decode(Buffer.fromChannelBuffer(buffer))} match {
          case Return(res) => Future.value(res)
          case Throw(ex) => throw ex
        }
      }
      case rep:DeleteRequest => trans.read flatMap {buffer =>
        val rep = ReplyHeader.decode(Buffer.fromChannelBuffer(buffer))
        processedReq.dequeue()
        Future.value(rep)
      }
      case rep:SetDataRequest => trans.read flatMap {buffer =>
        val rep = SetDataResponse.decode(Buffer.fromChannelBuffer(buffer))
        processedReq.dequeue()
        Future.value(rep)
      }
      case rep:GetDataRequest => trans.read flatMap {buffer =>
        val rep = GetDataResponse.decode(Buffer.fromChannelBuffer(buffer))
        processedReq.dequeue()
        Future.value(rep)
      }
      case rep:SyncRequest => trans.read flatMap {buffer =>
        val rep = SyncResponse.decode(Buffer.fromChannelBuffer(buffer))
        processedReq.dequeue()
        Future.value(rep)
      }
      case rep:SetACLRequest => trans.read flatMap {buffer =>
        val rep = SetACLResponse.decode(Buffer.fromChannelBuffer(buffer))
        processedReq.dequeue()
        Future.value(rep)
      }
      case rep:GetACLRequest => trans.read flatMap {buffer =>
        val rep = GetACLResponse.decode(Buffer.fromChannelBuffer(buffer))
        processedReq.dequeue()
        Future.value(rep)
      }
      case rep:GetChildrenRequest => trans.read flatMap {buffer =>
        val rep = GetChildrenResponse.decode(Buffer.fromChannelBuffer(buffer))
        processedReq.dequeue()
        Future.value(rep)
      }
      case rep:GetChildren2Request => trans.read flatMap {buffer =>
        val rep = GetChildren2Response.decode(Buffer.fromChannelBuffer(buffer))
        processedReq.dequeue()
        Future.value(rep)
      }
      case rep:SetWatchesRequest => trans.read flatMap {buffer =>
        val rep = ReplyHeader.decode(Buffer.fromChannelBuffer(buffer))
        processedReq.dequeue()
        Future.value(rep)
      }
      case rep:TransactionRequest => trans.read flatMap {buffer =>
        val rep = TransactionResponse.decode(Buffer.fromChannelBuffer(buffer))
        processedReq.dequeue()
        Future.value(rep)
      }
      case _ => throw new RuntimeException("READ REQUEST NOT SUPPORTED")
    }
  }

  def doWrite(req: Request): Future[Unit] = {
    processedReq.enqueue(req)
    trans.write(req.toChannelBuffer)
  }

  override def remoteAddress: SocketAddress = trans.remoteAddress
  override def localAddress: SocketAddress = trans.localAddress
  override def isOpen: Boolean = trans.isOpen
  override val onClose: Future[Throwable] = trans.onClose
  override def close(deadline: Time): Future[Unit] = trans.close(deadline)
}
