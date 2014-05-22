package com.twitter.finagle.exp.zookeeper.client

import com.twitter.util.{Promise, Future, Time, Closable}
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.exp.zookeeper._
import com.twitter.finagle.Service
import scala.collection.mutable
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.exp.zookeeper.transport.ZkTransport
import com.twitter.finagle.exp.zookeeper.ZookeeperDefinitions.opCode

trait Dispatcher extends Closable {
  val trans: Transport[ChannelBuffer, ChannelBuffer]
  private[this] val onClose = new Promise[Unit]
  protected var response: Response
  val processedReq = new mutable.SynchronizedQueue[Request]
  val zkTransporter = new ZkTransport(trans)
  val sessionManager = new SessionManager

  override def close(deadline: Time): Future[Unit] = {
    onClose.setDone()
    trans.close(deadline)
  }

  private[this] def loopRead(): Future[Response] = {
    processedReq.front match {
      case re: ConnectRequest => doRead(opCode.createSession)
      case re: PingRequest => doRead(opCode.ping)
      case re: CloseSessionRequest => doRead(opCode.closeSession)
      case re: CreateRequest => doRead(opCode.create)
      case re: DeleteRequest => doRead(opCode.delete)
      case re: ExistsRequest => doRead(opCode.exists)
      case re: GetACLRequest => doRead(opCode.getACL)
      case re: GetChildrenRequest => doRead(opCode.getChildren)
      case re: GetChildren2Request => doRead(opCode.getChildren2)
      case re: GetDataRequest => doRead(opCode.getData)
      case re: SetACLRequest => doRead(opCode.setACL)
      case re: SetDataRequest => doRead(opCode.setData)
      case re: SetWatchesRequest => doRead(opCode.setWatches)
      case re: SyncRequest => doRead(opCode.sync)
      case re: TransactionRequest => doRead(opCode.multi)
    }
  }

  private[this] def loopWrite(request: Request): Future[Unit] = {
    doWrite(request)
  }

  protected def loop(request: Request) = {
    loopWrite(request) onFailure (_ => close(Time.now))
    loopRead() onFailure (_ => close(Time.now)) onSuccess (response = _)
  }

  def doWrite(request: Request): Future[Unit] = {
    processedReq.enqueue(request)
    request match {
      case re: ConnectRequest => println("Connect request")
      case re: PingRequest => println("Ping Request")
      case re: CloseSessionRequest => println("Close session request"); sessionManager.stopPing
      case re: CreateRequest =>

    }
    zkTransporter.write(request)
  }

  def doRead(opCod: Int): Future[Response] = {
    opCod match {
      case -10 => println("read connect response"); sessionManager.startPing(zkTransporter.write(new PingRequest))
      case opCode.ping => println("Read ping response")
      case opCode.closeSession => println("Read Close response")
      case i: Int => println("Other type of response")
    }
    processedReq.dequeue()
    zkTransporter.read(opCod)
  }
}

class ClientDispatcher(val trans: Transport[ChannelBuffer, ChannelBuffer]) extends Service[Request, Response] with Dispatcher {
  override protected var response: Response = new EmptyResponse
  override def apply(request: Request): Future[Response] = {
    loop(request)

    Future(response)
  }
}
