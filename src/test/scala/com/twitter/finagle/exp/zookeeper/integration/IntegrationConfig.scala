package com.twitter.finagle.exp.zookeeper.integration

import com.twitter.finagle.exp.zookeeper.Zookeeper
import com.twitter.finagle.exp.zookeeper.client.ZkClient
import com.twitter.util.Await
import com.twitter.util.TimeConversions._
import java.net.{BindException, ServerSocket}
import org.scalatest.FunSuite

trait IntegrationConfig extends FunSuite{
  val ipAddress: String
  val port: Int
  val timeOut: Long

  def isPortAvailable: Boolean = try {
    val socket = new ServerSocket(port)
    socket.close()
    true
  } catch {
    case e: BindException => false
  }

  var client: Option[ZkClient] = None

  def newClient() {
    assume(!isPortAvailable, "A server is required for integration tests, see IntegrationConfig")
    client = {
      if (!isPortAvailable)
        Some(
          Zookeeper
            .withCanReadOnly()
            .withAutoWatchReset()
            .withAutoReconnect(Some(1.minute),Some(30.seconds), Some(10), Some(5))
            .withAutoRwServerSearch(Some(1.minute))
            .withAutoWatchReset()
            .withPreventiveSearch(Some(1.minute))
            .newRichClient(ipAddress + ":" + port)
        )
      else
        None
    }
  }

  def connect() { Await.ready(client.get.connect()) }
  def disconnect() { Await.ready(client.get.closeSession()) }
}
