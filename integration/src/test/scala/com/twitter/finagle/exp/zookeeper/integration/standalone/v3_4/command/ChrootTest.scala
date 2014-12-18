package com.twitter.finagle.exp.zookeeper.integration.standalone.v3_4.command

import java.util

import com.twitter.finagle.exp.zookeeper.Zookeeper
import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.CreateMode
import com.twitter.finagle.exp.zookeeper.data.Ids
import com.twitter.finagle.exp.zookeeper.integration.standalone.StandaloneIntegrationConfig
import com.twitter.util.Await
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ChrootTest extends FunSuite with StandaloneIntegrationConfig {
  test("Chroot works") {
    val clientWCh = Some(
      Zookeeper.client
        .withAutoReconnect()
        .withZkConfiguration(chroot = "/ch1")
        .newRichClient(ipAddress + ":" + port)
    )
    val client = Some(
      Zookeeper.client
        .withAutoReconnect()
        .newRichClient(ipAddress + ":" + port)
    )

    Await.ready(client.get.connect())
    Await.result(client.get.create(
      "/ch1",
      "hello".getBytes,
      Ids.OPEN_ACL_UNSAFE,
      CreateMode.PERSISTENT
    ))

    Await.ready(clientWCh.get.connect())

    val rep = for {
      _ <- clientWCh.get.create(
        "/ch2",
        "hello".getBytes,
        Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT)
      exists <- client.get.exists("/ch1", true)
      exists2 <- client.get.exists("/ch1/ch2", true)
      existsW <- clientWCh.get.exists("/ch2", true)
      getChildren <- client.get.getChildren("/ch1", true)
      getChildrenW <- clientWCh.get.getChildren("/", true)
      _ <- client.get.setData("/ch1", "HELLO".getBytes, -1)
      _ <- clientWCh.get.setData("/ch2", "HELLO1".getBytes, -1)
      getData <- client.get.getData("/ch1", false)
      getData2 <- client.get.getData("/ch1/ch2", false)
      getDataW <- clientWCh.get.getData("/ch2", false)
      _ <- clientWCh.get.delete("/ch2", -1)
      _ <- client.get.delete("/ch1", -1)
    } yield (exists, exists2, existsW, getChildren,
        getChildrenW, getData,
        getData2, getDataW)

    val (exists, exists2, existsW, getChildren,
    getChildrenW, getData,
    getData2, getDataW) = Await.result(rep)

    assert(exists.stat.isDefined)
    assert(exists.watcher.isDefined)
    assert(exists2.stat.isDefined)
    assert(exists2.watcher.isDefined)
    assert(existsW.stat.isDefined)
    assert(existsW.watcher.isDefined)

    Await.ready(exists.watcher.get.event)
    Await.ready(exists2.watcher.get.event)
    Await.ready(existsW.watcher.get.event)

    assert(util.Arrays.equals(getData.data, "HELLO".getBytes))
    assert(util.Arrays.equals(getData2.data, "HELLO1".getBytes))
    assert(util.Arrays.equals(getDataW.data, "HELLO1".getBytes))

    assert(getChildren.watcher.isDefined)
    assert(getChildrenW.watcher.isDefined)

    Await.ready(getChildren.watcher.get.event)
    Await.ready(getChildrenW.watcher.get.event)

    Await.ready(clientWCh.get.disconnect())
    Await.ready(client.get.disconnect())

    Await.ready(clientWCh.get.close())
    Await.ready(client.get.close())
  }
}