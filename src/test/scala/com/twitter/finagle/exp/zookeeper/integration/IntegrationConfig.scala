package com.twitter.finagle.exp.zookeeper.integration

import java.net.{BindException, ServerSocket}
import com.twitter.finagle.exp.zookeeper.client.ClientWrapper
import com.twitter.util.Await

trait IntegrationConfig {
  val ipAddress: String
  val port: Int
  val timeOut: Long

  lazy val isPortAvailable = try {
    val socket = new ServerSocket(port)
    socket.close()
    true
  } catch {
    case e: BindException => false
  }

  lazy val client: Option[ClientWrapper] = {
    if (!isPortAvailable)
      Some(ClientWrapper.newClient(ipAddress + ":" + port, timeOut))
    else
      None
  }

  def connect = Await.result(client.get.connect)
  def disconnect = Await.result(client.get.disconnect)
}
