package com.twitter.finagle.exp.zookeeper.integration.quorum.v3_4.extra

import com.twitter.finagle.exp.zookeeper.integration.quorum.QuorumIntegrationConfig
import com.twitter.util.Await
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class EnsembleTest extends FunSuite with QuorumIntegrationConfig {
  test("change host") {
    newClients()
    connectClients()

    val oldCli = client1.get.connectionManager.currentHost.get
    Await.ready{
      client1.get.changeHost(Some("127.0.0.1:2182"))
    }
    assert(client1.get.connectionManager.currentHost.isDefined)
    assert(client1.get.connectionManager.currentHost.get === "127.0.0.1:2182")
    assert(client1.get.connectionManager.currentHost.get != oldCli)

    val oldCli2 = client2.get.connectionManager.currentHost.get
    Await.ready{
      client2.get.changeHost(Some("127.0.0.1:2181"))
    }
    assert(client2.get.connectionManager.currentHost.isDefined)
    assert(client2.get.connectionManager.currentHost.get === "127.0.0.1:2181")
    assert(client2.get.connectionManager.currentHost.get != oldCli2)

    val oldCli3 = client3.get.connectionManager.currentHost.get
    Await.ready{
      client3.get.changeHost(Some("127.0.0.1:2182"))
    }
    assert(client3.get.connectionManager.currentHost.isDefined)
    assert(client3.get.connectionManager.currentHost.get === "127.0.0.1:2182")
    assert(client3.get.connectionManager.currentHost.get != oldCli3)

    disconnectClients()
    closeServices()
  }

  test("add hosts") {
    newClients()
    connectClients()

    client1.get.addHosts("127.0.0.1:2183")
    assert(client1.get.connectionManager.hostProvider.serverList.contains("127.0.0.1:2183"))
    val oldCli = client1.get.connectionManager.currentHost.get
    Await.ready{
      client1.get.changeHost()
    }
    assert(client1.get.connectionManager.currentHost.isDefined)
    assert(client1.get.connectionManager.currentHost.get === "127.0.0.1:2183")
    assert(client1.get.connectionManager.currentHost.get != oldCli)

    disconnectClients()
    closeServices()
  }

  test("remove hosts") {
    newClients()
    connectClients()

    client1.get.addHosts("127.0.0.1:2183")
    val oldCli = client1.get.connectionManager.currentHost.get
    Await.result{
      client1.get.removeHosts("127.0.0.1:2181")
    }
    assert(client1.get.connectionManager.currentHost.isDefined)
    assert(client1.get.connectionManager.currentHost.get === "127.0.0.1:2183")
    assert(client1.get.connectionManager.currentHost.get != oldCli)
    assert(!client1.get.connectionManager.hostProvider.serverList.contains("127.0.0.1:2181"))

    disconnectClients()
    closeServices()
  }
}