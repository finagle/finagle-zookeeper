package com.twitter.finagle.exp.zookeeper

import com.twitter.finagle.client._
import com.twitter.finagle.dispatch.PipeliningDispatcher
import com.twitter.finagle.exp.zookeeper.client.Params.{AutoReconnect, ZkConfiguration}
import com.twitter.finagle.exp.zookeeper.client.{Params, ZkClient, ZkDispatcher}
import com.twitter.finagle.exp.zookeeper.transport.{BufTransport, NettyTrans, ZkTransport, ZookeeperTransporter}
import com.twitter.finagle.{Client, Name, ServiceFactory, Stack}
import com.twitter.io.Buf
import com.twitter.util.Duration
import com.twitter.util.TimeConversions._
import org.jboss.netty.buffer.ChannelBuffer

/**
 * -- ZkDispatcher
 * The zookeeper protocol allows the server to send notifications
 * with model is not basically supported by Finagle, that's the reason
 * why we are using a dedicated dispatcher based on SerialClientDispatcher and
 * GenSerialClientDispatcher.
 *
 * -- ZkTransport
 * We need to frame Rep here, that's why we use ZkTransport
 * which is responsible framing Buf and converting to Buf.
 * It is also reading the request by copying the corresponding buffer in a Buf.
 */
trait ZookeeperRichClient {self: com.twitter.finagle.Client[ReqPacket, RepPacket] =>
  val params: Stack.Params
  def newRichClient(dest: String, label: String): ZkClient = {
    val ZkConfiguration(autoWatch, canRO, chRoot, sessTime) = params[ZkConfiguration]
    val AutoReconnect(autoReco, autoRw, prevS, tba, tblc, mcr, mra) = params[AutoReconnect]
    ZkClient(
      autowatchReset = autoWatch,
      autoRecon = autoReco,
      canBeReadOnly = canRO,
      chrootPath = chRoot,
      hostList = dest,
      maxConsecRetries = mcr,
      maxReconAttempts = mra,
      serviceLabel = Some(label),
      sessTimeout = sessTime,
      timeBtwnAttempts = tba,
      timeBtwnLinkCheck = tblc,
      timeBtwnRwSrch = autoRw,
      timeBtwnPrevSrch = prevS
    )
  }

  def newRichClient(dest: String): ZkClient = {
    val ZkConfiguration(autoWatch, canRO, chRoot, sessTime) = params[ZkConfiguration]
    val AutoReconnect(autoReco, autoRw, prevS, tba, tblc, mcr, mra) = params[AutoReconnect]
    ZkClient(
      autowatchReset = autoWatch,
      autoRecon = autoReco,
      canBeReadOnly = canRO,
      chrootPath = chRoot,
      hostList = dest,
      maxConsecRetries = mcr,
      maxReconAttempts = mra,
      serviceLabel = None,
      sessTimeout = sessTime,
      timeBtwnAttempts = tba,
      timeBtwnLinkCheck = tblc,
      timeBtwnRwSrch = autoRw,
      timeBtwnPrevSrch = prevS
    )
  }
}

object ZookeeperStackClient extends StackClient[ReqPacket, RepPacket] {
  type In = ChannelBuffer
  type Out = ChannelBuffer
  protected val newTransporter = ZookeeperTransporter(_)
  protected val newDispatcher: Stack.Params => Dispatcher = { params =>
    trans => new ZkDispatcher(new ZkTransport(trans))
  }

  override def newClient(
    dest: Name,
    label: String
  ): ServiceFactory[ReqPacket, RepPacket] = {
    super.newClient(dest, label)
  }
}

class ZookeeperClient(client: StackClient[ReqPacket, RepPacket])
  extends StackClientLike[ReqPacket, RepPacket, ZookeeperClient](client)
  with ZookeeperRichClient {
  val params = client.params
  protected def newInstance(client: StackClient[ReqPacket, RepPacket]) =
    new ZookeeperClient(client)

  def withAutoReconnect(
    autoRwServerSearch: Option[Duration] = Some(1.minute),
    preventiveSearch: Option[Duration] = Some(10.minutes),
    timeBetweenAttempts: Duration = 30.seconds,
    timeBetweenLinkCheck: Option[Duration] = Some(30.seconds),
    maxConsecutiveRetries: Int = 10,
    maxReconnectAttempts: Int = 5
  ) = configured(
    Params.AutoReconnect(
      true,
      autoRwServerSearch,
      preventiveSearch,
      timeBetweenAttempts,
      timeBetweenLinkCheck,
      maxConsecutiveRetries,
      maxReconnectAttempts
    )
  )

  def withZkConfiguration(
    autoWatchReset: Boolean = true,
    canReadOnly: Boolean = true,
    chroot: String = "",
    sessionTimeout: Duration = 3000.milliseconds
  ) = configured(
    Params.ZkConfiguration(
      autoWatchReset,
      canReadOnly,
      chroot,
      sessionTimeout
    )
  )
}

object Zookeeper extends ZookeeperClient(
  ZookeeperStackClient.configured(
    DefaultPool.Param(
      low = 0, high = 1, bufferSize = 0,
      idleTime = Duration.Top,
      maxWaiters = Int.MaxValue
    )
  )
)

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
    newDispatcher = new PipeliningDispatcher(_)
  )
)

private[finagle] object BufClient extends Client[Buf, Buf] {
  override def newClient(dest: Name, label: String): ServiceFactory[Buf, Buf] =
    SimpleClient.newClient(dest, label)

  def newSimpleClient(dest: String): ServiceFactory[Buf, Buf] =
    SimpleClient.newClient(dest)
}