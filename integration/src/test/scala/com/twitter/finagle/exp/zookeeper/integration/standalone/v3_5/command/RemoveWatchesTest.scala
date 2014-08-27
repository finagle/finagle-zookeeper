package com.twitter.finagle.exp.zookeeper.integration.standalone.v3_5.command

import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.CreateMode
import com.twitter.finagle.exp.zookeeper.data.Ids
import com.twitter.finagle.exp.zookeeper.integration.standalone.StandaloneIntegrationConfig
import com.twitter.util.Await
import org.scalatest.FunSuite

class RemoveWatchesTest extends FunSuite with StandaloneIntegrationConfig {
  // TODO RemoveWatchesTest.java
  test("Remove a correct watch") {
    newClient()
    connect()

    val rep = Await.result {
      for {
        exists <- client.get.exists("/hello", true)
        remove <- client.get.removeWatches(exists.watcher.get, false)
        create <- client.get.create(
          "/hello",
          "".getBytes,
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.EPHEMERAL
        )
      } yield exists
    }

    assert(!rep.watcher.get.event.isDefined)

    disconnect()
    Await.ready(client.get.close())
  }

  test("Not remove a non-existing watcher") {
    newClient()
    connect()

    intercept[Exception] {
      Await.result {
        for {
          exists <- client.get.exists("/hello", true)
          create <- client.get.create(
            "/hello",
            "".getBytes,
            Ids.OPEN_ACL_UNSAFE,
            CreateMode.EPHEMERAL
          )
          remove <- client.get.removeWatches(exists.watcher.get, false)
        } yield remove
      }
    }

    disconnect()
    Await.ready(client.get.close())
  }
}
