package com.twitter.finagle.exp.zookeeper.integration.quorum.command

import com.twitter.finagle.exp.zookeeper.NoNodeException
import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.CreateMode
import com.twitter.finagle.exp.zookeeper.data.Ids
import com.twitter.finagle.exp.zookeeper.integration.quorum.QuorumIntegrationConfig
import com.twitter.util.Await
import java.util
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CRUDTest extends FunSuite with QuorumIntegrationConfig {
  test("get the same node on all the clients") {
    newClients()
    connectClients()

    val ret = Await.result {
      for {
        _ <- client1.get.create(
          "/node1",
          "HELLO".getBytes,
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.EPHEMERAL
        )
        get1 <- client1.get.getData("/node1")
        _ <- client2.get.sync("/node1")
        get2 <- client2.get.getData("/node1")
        _ <- client3.get.sync("/node1")
        get3 <- client3.get.getData("/node1")
      } yield (get1, get2, get3)
    }

    val (get1, get2, get3) = ret
    assert(get1.stat === get2.stat)
    assert(get2.stat === get3.stat)

    disconnectClients()
    closeServices()
  }

  test("set node") {
    newClients()
    connectClients()

    val ret = Await.result {
      for {
        _ <- client1.get.create(
          "/node1",
          "HELLO".getBytes,
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.EPHEMERAL
        )
        _ <- client2.get.setData("/node1", "CHANGED".getBytes, -1)
        get <- client2.get.getData("/node1")
        _ <- client3.get.setData("/node1", "CHANGED".getBytes, -1)
        _ <- client3.get.sync("/node1")
        get1 <- client3.get.getData("/node1")
      } yield (get, get1)
    }

    val (get, get1) = ret
    assert(util.Arrays.equals(get.data, get1.data))

    disconnectClients()
    closeServices()
  }

  test("create, getData, setData, delete") {
    newClients()
    connectClients()

    val ret = Await.result {
      for {
        _ <- client1.get.create(
          "/node1",
          "HELLO".getBytes,
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.EPHEMERAL
        )
        _ <- client2.get.setData("/node1", "CHANGED".getBytes, -1)
        _ <- client1.get.sync("/node1")
        get <- client1.get.getData("/node1")
        _ <- client3.get.setData("/node1", "CHANGED".getBytes, -1)
        _ <- client3.get.sync("/node1")
        get1 <- client3.get.getData("/node1")
        _ <- client3.get.delete("/node1", -1)
      } yield (get, get1)
    }

    val (get, get1) = ret
    assert(util.Arrays.equals(get.data, get1.data))

    intercept[NoNodeException] {
      Await.result {
        client1.get.getData("/node1")
      }
    }

    disconnectClients()
    closeServices()
  }
}