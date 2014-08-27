package com.twitter.finagle.exp.zookeeper.integration.standalone.v3_5.command

import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.CreateMode
import com.twitter.finagle.exp.zookeeper.data.Ids
import com.twitter.finagle.exp.zookeeper.integration.standalone.StandaloneIntegrationConfig
import com.twitter.finagle.exp.zookeeper.watcher.Watch.WatcherType
import com.twitter.util.Await
import org.scalatest.FunSuite

class CheckWatchesTest extends FunSuite with StandaloneIntegrationConfig {
  test("Check correct exists watch") {
    newClient()
    connect()

    Await.result {
      for {
        _ <- client.get.exists("/hello", true)
        check <- client.get.checkWatches("/hello", WatcherType.DATA)
      } yield check
    }

    disconnect()
    Await.ready(client.get.close())
  }

  test("Check correct getData watch") {
    newClient()
    connect()

    Await.result {
      for {
        _ <- client.get.create("/hello", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
        _ <- client.get.getData("/hello", true)
        check <- client.get.checkWatches("/hello", WatcherType.DATA)
      } yield check
    }

    disconnect()
    Await.ready(client.get.close())
  }

  test("Check correct getChildren watch") {
    newClient()
    connect()

    Await.result {
      for {
        _ <- client.get.create("/hello", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
        _ <- client.get.getChildren("/hello", true)
        check <- client.get.checkWatches("/hello", WatcherType.CHILDREN)
      } yield check
    }

    disconnect()
    Await.ready(client.get.close())
  }

  test("Check correct getChildren2 watch") {
    newClient()
    connect()

    Await.result {
      for {
        _ <- client.get.create("/hello", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
        _ <- client.get.getChildren2("/hello", true)
        check <- client.get.checkWatches("/hello", WatcherType.CHILDREN)
      } yield check
    }

    disconnect()
    Await.ready(client.get.close())
  }

  test("Check correct getChildren watch with Watcher type Any") {
    newClient()
    connect()

    Await.result {
      for {
        _ <- client.get.create("/hello", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
        _ <- client.get.getChildren("/hello", true)
        check <- client.get.checkWatches("/hello", WatcherType.ANY)
      } yield check
    }

    disconnect()
    Await.ready(client.get.close())
  }

  test("Check correct watches with watcher type Any") {
    newClient()
    connect()

    val ret = Await.result {
      for {
        _ <- client.get.create("/hello", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
        _ <- client.get.getChildren("/hello", true)
        _ <- client.get.getData("/hello", true)
        check <- client.get.checkWatches("/hello", WatcherType.CHILDREN)
        check1 <- client.get.checkWatches("/hello", WatcherType.DATA)
        check2 <- client.get.checkWatches("/hello", WatcherType.ANY)
      } yield (check, check1, check2)
    }

    val (check, check1, check2) = ret

    disconnect()
    Await.ready(client.get.close())
  }
}
