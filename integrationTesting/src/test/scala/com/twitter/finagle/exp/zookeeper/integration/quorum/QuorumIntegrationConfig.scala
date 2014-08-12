package com.twitter.finagle.exp.zookeeper.integration.quorum

import com.twitter.finagle.exp.zookeeper.Zookeeper
import com.twitter.finagle.exp.zookeeper.client.ZkClient
import com.twitter.util.TimeConversions._
import com.twitter.util.{Await, Duration}
import java.net.{BindException, ServerSocket}
import org.scalatest.FunSuite

trait QuorumIntegrationConfig extends FunSuite {
  val ipAddress: String = "127.0.0.1"
  val timeOut: Duration = 3000.milliseconds

  def isPortAvailable(port: Int): Boolean = try {
    val socket = new ServerSocket(port)
    socket.close()
    true
  } catch {
    case e: BindException => false
  }

  var client1: Option[ZkClient] = None
  var client2: Option[ZkClient] = None
  var client3: Option[ZkClient] = None

  def newClients() {
    assume(!isPortAvailable(2181), "A server is required for integration tests, see IntegrationConfig")
    assume(!isPortAvailable(2182), "A server is required for integration tests, see IntegrationConfig")
    assume(!isPortAvailable(2183), "A server is required for integration tests, see IntegrationConfig")
    client1 = {
      if (!isPortAvailable(2181))
        Some(
          Zookeeper
            .withAutoReconnect()
            .withZkConfiguration(sessionTimeout = timeOut)
            .newRichClient(ipAddress + ":" + 2181)
        )
      else
        None
    }
    client2 = {
      if (!isPortAvailable(2182))
        Some(
          Zookeeper
            .withAutoReconnect()
            .withZkConfiguration(sessionTimeout = timeOut)
            .newRichClient(ipAddress + ":" + 2182)
        )
      else
        None
    }
    client3 = {
      if (!isPortAvailable(2183))
        Some(
          Zookeeper
            .withAutoReconnect()
            .withZkConfiguration(sessionTimeout = timeOut)
            .newRichClient(ipAddress + ":" + 2183)
        )
      else
        None
    }
  }

  def connectClients() {
    Await.ready{
      client1.get.connect() before client2.get.connect() before
        client3.get.connect()
    }
  }

  def disconnectClients() {
    Await.ready{
      client1.get.closeSession() before client2.get.closeSession() before
        client3.get.closeSession()
    }
  }

  def closeServices() {
    Await.ready{
      client1.get.closeService() before client2.get.closeService() before
        client3.get.closeService()
    }
  }
}