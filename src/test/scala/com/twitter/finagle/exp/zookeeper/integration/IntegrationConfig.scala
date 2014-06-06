package com.twitter.finagle.exp.zookeeper.integration

import java.net.{BindException, ServerSocket}
import com.twitter.finagle.exp.zookeeper.client.ZkClient
import com.twitter.util.Await
import com.twitter.finagle.exp.zookeeper.ZooKeeper

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

  var client: Option[ZkClient] = None

  def newClient() {
    client = {
      if (!isPortAvailable)
        Some(ZooKeeper.newRichClient(ipAddress + ":" + port))
      else
        None
    }
  }

  def connect() { Await.ready(client.get.connect()) }
  def disconnect() { Await.ready(client.get.closeSession()) }
}
