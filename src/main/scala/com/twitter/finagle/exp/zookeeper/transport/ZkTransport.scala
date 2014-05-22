package com.twitter.finagle.exp.zookeeper.transport

import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.transport.Transport
import com.twitter.util._
import java.net.SocketAddress
import com.twitter.finagle.exp.zookeeper._
import com.twitter.finagle.exp.zookeeper.client.Client

case class ZkTransport(
  trans: Transport[ChannelBuffer, ChannelBuffer]
  ) extends Transport[Request, Response] {
  val logger = Client.getLogger

  /**
   * When receiving a Request
   * @param request the Request to encode
   * @return Future[Unit] when the message is correctly written
   */
  override def write(request: Request): Future[Unit] = trans.write(request.toChannelBuffer)

  def read(opCode: Int): Future[Response] = {
    trans.read() flatMap { buffer =>
      val br = BufferReader(buffer)
      decoder(opCode, br)
    }
  }

  def decoder(opCode: Int, br: BufferReader): Future[Response] = opCode match {
    case ZookeeperDefinitions.opCode.createSession =>
      val connectRep = ConnectResponse.decode(br)
      Future.value(connectRep)

    case ZookeeperDefinitions.opCode.closeSession =>
      val header = ReplyHeader.decode(br)
      Future.value(header)

    case ZookeeperDefinitions.opCode.create =>
      val header = ReplyHeader.decode(br)
      val rep = CreateResponse.decode(br)
      Future.value(rep)

    case ZookeeperDefinitions.opCode.exists =>
      val header = ReplyHeader.decode(br)
      Try {ExistsResponse.decode(br)} match {
        case Return(res) =>
          Future.value(res)
        case Throw(ex) => throw ex
      }

    case ZookeeperDefinitions.opCode.delete =>
      val header = ReplyHeader.decode(br)
      Future(new EmptyResponse)

    case ZookeeperDefinitions.opCode.setData =>
      val header = ReplyHeader.decode(br)
      val rep = SetDataResponse.decode(br)
      Future.value(rep)

    case ZookeeperDefinitions.opCode.getData =>
      val header = ReplyHeader.decode(br)
      val rep = GetDataResponse.decode(br)
      Future.value(rep)

    case ZookeeperDefinitions.opCode.ping =>
      val header = ReplyHeader.decode(br)
      Future.value(header)

    case ZookeeperDefinitions.opCode.sync =>
      val header = ReplyHeader.decode(br)
      val rep = SyncResponse.decode(br)
      Future.value(rep)

    case ZookeeperDefinitions.opCode.setACL =>
      val header = ReplyHeader.decode(br)
      val rep = SetACLResponse.decode(br)
      Future.value(rep)

    case ZookeeperDefinitions.opCode.getACL =>
      val header = ReplyHeader.decode(br)
      val rep = GetACLResponse.decode(br)
      Future.value(rep)

    case ZookeeperDefinitions.opCode.getChildren =>
      val header = ReplyHeader.decode(br)
      val rep = GetChildrenResponse.decode(br)
      Future.value(rep)

    case ZookeeperDefinitions.opCode.getChildren2 =>
      val header = ReplyHeader.decode(br)
      val rep = GetChildren2Response.decode(br)
      Future.value(rep)

    case ZookeeperDefinitions.opCode.setWatches =>
      val header = ReplyHeader.decode(br)
      Future.value(header)

    case ZookeeperDefinitions.opCode.multi =>
      val header = ReplyHeader.decode(br)
      val rep = TransactionResponse.decode(br)
      Future.value(rep)

    case 666 => println("THIS IS A NOTIFICATION"); throw new RuntimeException("FUCK NOTIFICATIONS")
  }

  override def read(): Future[Response] = throw new RuntimeException("Classic read operation not supported, use read(opCode: Int)")
  override def remoteAddress: SocketAddress = trans.remoteAddress
  override def localAddress: SocketAddress = trans.localAddress
  override def isOpen: Boolean = trans.isOpen
  override val onClose: Future[Throwable] = trans.onClose
  override def close(deadline: Time): Future[Unit] = trans.close(deadline)
}
