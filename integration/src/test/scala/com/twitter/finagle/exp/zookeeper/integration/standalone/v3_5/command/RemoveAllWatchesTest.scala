package com.twitter.finagle.exp.zookeeper.integration.standalone.v3_5.command

import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.CreateMode
import com.twitter.finagle.exp.zookeeper.data.Ids
import com.twitter.finagle.exp.zookeeper.integration.standalone.StandaloneIntegrationConfig
import com.twitter.finagle.exp.zookeeper.watcher.Watch.WatcherType
import com.twitter.util.Await
import org.scalatest.FunSuite

class RemoveAllWatchesTest extends FunSuite with StandaloneIntegrationConfig {
  test("Remove a correct watch") {
    newClient()
    connect()

    val rep = Await.result {
      for {
        exists <- client.get.exists("/hello", true)
        remove <- client.get.removeAllWatches("/hello", WatcherType.DATA, false)
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

  test("Remove a getData and exists watches") {
    newClient()
    connect()

    val rep = Await.result {
      for {
        _ <- client.get.create(
          "/hello",
          "".getBytes,
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT
        )
        exists <- client.get.exists("/hello", true)
        getdata <- client.get.getData("/hello", true)
        getchildren <- client.get.getChildren("/hello", true)
        _ <- client.get.removeAllWatches("/hello", WatcherType.DATA, false)
        _ <- client.get.create(
          "/hello/1",
          "".getBytes,
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.EPHEMERAL
        )
        _ <- client.get.delete("/hello/1", -1)
        _ <- client.get.delete("/hello", -1)
      } yield (exists, getdata, getchildren)
    }

    val (exists, getdata, children) = rep
    assert(!exists.watcher.get.event.isDefined)
    assert(!getdata.watcher.get.event.isDefined)
    assert(children.watcher.get.event.isDefined)

    disconnect()
    Await.ready(client.get.close())
  }
}
