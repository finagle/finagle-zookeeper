package com.twitter.finagle.exp.zookeeper.integration.quorum.extra

import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.CreateMode
import com.twitter.finagle.exp.zookeeper.data.Ids
import com.twitter.finagle.exp.zookeeper.integration.quorum.QuorumIntegrationConfig
import com.twitter.util.Await
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ReconnectionTest extends FunSuite with QuorumIntegrationConfig {
  test("Reconnect with the same session to another server") {
    newClients()
    connectClients()

    Await.ready {
      client1.get.create("/node1", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
    }

    Await.ready {
      client1.get.reconnectWithSession(Some("127.0.0.1:2183"), 0)
    }

    val res2 = Await.result {
      client2.get.getData("/node1")
    }
    val res1 = Await.result {
      client1.get.getData("/node1")
    }

    assert(res1.stat === res2.stat)

    disconnectClients()
    closeServices()
  }
}