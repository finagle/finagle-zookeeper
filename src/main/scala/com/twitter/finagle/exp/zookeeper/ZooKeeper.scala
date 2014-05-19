package com.twitter.finagle.exp.zookeeper

import com.twitter.finagle.service.{TimeoutFilter, RetryPolicy, RetryingFilter}
import com.twitter.finagle.exp.zookeeper.transport.{ZkTransport, ZooKeeperTransporter}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.client.{Bridge, DefaultClient}
import com.twitter.finagle.dispatch.{SerialClientDispatcher, PipeliningDispatcher}
import com.twitter.finagle.{ServiceFactory, Name}
import com.twitter.conversions.time._
import com.twitter.finagle
import com.twitter.finagle.exp.zookeeper.client.Client
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.transport.Transport

trait ZKRichClient {
  self: finagle.Client[Request, Response] =>
  def newRichClient(dest: Name, label: String): Client =
    client.Client(newClient(dest, label))

  def newRichClient(dest: String): Client =
    client.Client(newClient(dest))
}

class ZKClient extends com.twitter.finagle.Client[Request, Response]
with ZKRichClient {
  val defaultClient = new DefaultClient[Request, Response](
    name = "zookeeper",
    endpointer = {
      val bridge = Bridge[Request, Response, Request, Response](
        ZooKeeperTransporter(_, _) map {ZkTransport(_)},
        newDispatcher = new SerialClientDispatcher(_)
      )
      (sa, sr) => bridge(sa, sr)
    }
  )

  override def newClient(dest: Name, label: String): ServiceFactory[Request, Response] =
    defaultClient.newClient(dest, label)
}

object Filter {
  val retry = new RetryingFilter[Request, Response](
    retryPolicy = RetryPolicy.tries(3),
    timer = DefaultTimer.twitter)

  val timeout = new TimeoutFilter[Request, Response](
    timeout = 3.seconds,
    timer = DefaultTimer.twitter)
}


object ZooKeeper extends ZKClient()
