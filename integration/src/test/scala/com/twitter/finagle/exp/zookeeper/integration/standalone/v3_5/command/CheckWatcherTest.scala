package com.twitter.finagle.exp.zookeeper.integration.standalone.v3_5.command

import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.CreateMode
import com.twitter.finagle.exp.zookeeper.data.Ids
import com.twitter.finagle.exp.zookeeper.integration.standalone.StandaloneIntegrationConfig
import com.twitter.util.Await
import org.scalatest.FunSuite

class CheckWatcherTest extends FunSuite with StandaloneIntegrationConfig {
  test("Should check a watcher without error") {
    newClient()
    connect()

    Await.result {
      for {
        exists <- client.get.exists("/hello", true)
        check <- client.get.checkWatcher(exists.watcher.get)
      } yield check
    }

    disconnect()
    Await.ready(client.get.close())
  }

  test("Should not check a watcher correctly") {
    newClient()
    connect()

    intercept[Exception] {
      Await.result {
        for {
          exists <- client.get.exists("/hello", true)
          create <- client.get.create("/hello", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
          check <- client.get.checkWatcher(exists.watcher.get)
        } yield check
      }
    }

    disconnect()
    Await.ready(client.get.close())
  }
}