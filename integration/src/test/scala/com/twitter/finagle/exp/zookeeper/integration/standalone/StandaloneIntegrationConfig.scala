package com.twitter.finagle.exp.zookeeper.integration.standalone

import com.twitter.finagle.exp.zookeeper.Zookeeper
import com.twitter.finagle.exp.zookeeper.client.ZkClient
import com.twitter.util.TimeConversions._
import com.twitter.util.{Await, Duration}
import java.net.{BindException, ServerSocket}
import org.scalatest.FunSuite

trait StandaloneIntegrationConfig extends FunSuite {
  val ipAddress: String = "127.0.0.1"
  val port: Int = 2181
  val timeOut: Duration = 3000.milliseconds

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
            .withAutoReconnect()
            .withZkConfiguration(sessionTimeout = timeOut)
            .newRichClient(ipAddress + ":" + port)
        )
      else
        None
    }
  }

  def connect() = { Await.result(client.get.connect()) }
  def disconnect() = { Await.result(client.get.disconnect()) }
}