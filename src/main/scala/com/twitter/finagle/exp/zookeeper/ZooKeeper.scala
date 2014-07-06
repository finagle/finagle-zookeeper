package com.twitter.finagle.exp.zookeeper

import com.twitter.finagle.{Client, Name, ServiceFactory}
import com.twitter.finagle.client.{Bridge, DefaultClient}
import com.twitter.finagle.dispatch.PipeliningDispatcher
import com.twitter.finagle.exp.zookeeper.client.{ZkClient, ZkDispatcher}
import com.twitter.finagle.exp.zookeeper.transport.{BufTransport, NettyTrans, ZkTransport}
import com.twitter.io.Buf

/**
 * -- ZkDispatcher
 * The zookeeper protocol allows the server to send notifications
 * with model is not basically supported by Finagle, that's the reason
 * why we are using a dedicated dispatcher based on SerialClientDispatcher and
 * GenSerialClientDispatcher.
 *
 * -- ZkTransport
 * We need to frame Rep here, that's why we use ZkTransport
 * which is responsible framing Buf and converting to ChannelBuffer.
 * It is also reading the request by copying the corresponding buffer in a Buf.
 */
object ZooKeeperClient extends DefaultClient[ReqPacket, RepPacket](
  name = "zookeeper",
  pool = { _ => identity },
  endpointer = Bridge[Buf, Buf, ReqPacket, RepPacket](
    NettyTrans(_, _) map { new ZkTransport(_) }, new ZkDispatcher(_)))

object ZooKeeper extends Client[ReqPacket, RepPacket] {
  def newClient(
    name: Name,
    label: String
    ): ServiceFactory[ReqPacket, RepPacket] =
    ZooKeeperClient.newClient(name, label)

  def newRichClient(hostsList: String): ZkClient =
    new ZkClient(hostList = hostsList)
}

/**
 * Simple client is used to send isro request ( not framed request )
 * and also to send connect and close requests to old servers ( version < 3.4.0 )
 * ( read-only mode not supported )
 * This is basically a Buf client, sending Buf and receiving Buf
 */
private[finagle] object SimpleClient extends DefaultClient[Buf, Buf](
  name = "isroClient",
  endpointer = Bridge[Buf, Buf, Buf, Buf](
    NettyTrans(_, _) map { new BufTransport(_) },
    newDispatcher = new PipeliningDispatcher(_)))

private[finagle] object BufClient extends Client[Buf, Buf] {
  override def newClient(dest: Name, label: String): ServiceFactory[Buf, Buf] =
    SimpleClient.newClient(dest, label)

  def newSimpleClient(dest: String): ServiceFactory[Buf, Buf] =
    SimpleClient.newClient(dest)
}