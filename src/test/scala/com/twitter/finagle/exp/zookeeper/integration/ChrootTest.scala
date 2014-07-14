package com.twitter.finagle.exp.zookeeper.integration

import com.twitter.finagle.exp.zookeeper.Zookeeper
import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.CreateMode
import com.twitter.finagle.exp.zookeeper.data.Ids
import com.twitter.util.TimeConversions._
import com.twitter.util.{Await, Duration}
import java.util
import org.scalatest.FunSuite

class ChrootTest extends FunSuite with IntegrationConfig {
  test("Chroot works") {
    val ipAddress: String = "127.0.0.1"
    val port: Int = 2181
    val timeOut: Duration = 3000.milliseconds
    val clientWCh = Some(
      Zookeeper
        .withCanReadOnly()
        .withAutoWatchReset()
        .withAutoReconnect(Some(1.minute), Some(30.seconds), Some(10), Some(5))
        .withAutoRwServerSearch(Some(1.minute))
        .withAutoWatchReset()
        .withChroot("/ch1")
        .withPreventiveSearch(Some(1.minute))
        .withSessionTimeout(timeOut)
        .newRichClient(ipAddress + ":" + port)
    )
    val client = Some(
      Zookeeper
        .withCanReadOnly()
        .withAutoWatchReset()
        .withAutoReconnect(Some(1.minute), Some(30.seconds), Some(10), Some(5))
        .withAutoRwServerSearch(Some(1.minute))
        .withAutoWatchReset()
        .withPreventiveSearch(Some(1.minute))
        .withSessionTimeout(timeOut)
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
      createW <- clientWCh.get.create(
        "/ch2",
        "hello".getBytes,
        Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT)
      exists <- client.get.exists("/ch1", true)
      exists2 <- client.get.exists("/ch1/ch2", true)
      existsW <- clientWCh.get.exists("/ch2", true)
      getChildren <- client.get.getChildren("/ch1", true)
      getChildrenW <- clientWCh.get.getChildren("/", true)
      setData <- client.get.setData("/ch1", "HELLO".getBytes, -1)
      setDataW <- clientWCh.get.setData("/ch2", "HELLO1".getBytes, -1)
      getData <- client.get.getData("/ch1", false)
      getData2 <- client.get.getData("/ch1/ch2", false)
      getDataW <- clientWCh.get.getData("/ch2", false)
      deleteW <- clientWCh.get.delete("/ch2", -1)
      delete <- client.get.delete("/ch1", -1)
    } yield (createW, exists, exists2, existsW, getChildren,
        getChildrenW, setData, setDataW, getData,
        getData2, getDataW, deleteW, delete)

    val (createW, exists, exists2, existsW, getChildren,
    getChildrenW, setData, setDataW, getData,
    getData2, getDataW, deleteW, delete) = Await.result(rep)

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

    Await.ready(clientWCh.get.closeSession())
    Await.ready(client.get.closeSession())

    Await.ready(clientWCh.get.closeService())
    Await.ready(client.get.closeService())
  }
}