package com.twitter.finagle.exp.zookeeper.integration.standalone.extra

import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.CreateMode
import com.twitter.finagle.exp.zookeeper.data.Ids
import com.twitter.finagle.exp.zookeeper.integration.standalone.StandaloneIntegrationConfig
import com.twitter.util.Await
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class LocalSessionUpdateTest extends FunSuite with StandaloneIntegrationConfig {
  test("updates local session") {
    newClient()
    connect()

    val res = for {
      _ <- client.get.create("/node1", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
      _ <- client.get.create("/node2", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
      exi <- client.get.exists("/node1", watch = false)
      exi2 <- client.get.exists("/node2", watch = false)
    } yield (exi, exi2)

    val (exi, exi2) = Await.result(res)

    assert(exi2.stat.get.czxid - exi.stat.get.czxid === 1L)

    disconnect()
    Await.ready(client.get.closeService())
  }
}
