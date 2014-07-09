package com.twitter.finagle.exp.zookeeper.integration

import java.net.{BindException, ServerSocket}
import com.twitter.finagle.exp.zookeeper.client.ZkClient
import com.twitter.util.Await
import com.twitter.finagle.exp.zookeeper.ZooKeeper
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
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
        Some(ZooKeeper.newRichClient(ipAddress + ":" + port))
      else
        None
    }
  }

  def connect() { Await.ready(client.get.connect()) }
  def disconnect() { Await.ready(client.get.closeSession()) }
}
