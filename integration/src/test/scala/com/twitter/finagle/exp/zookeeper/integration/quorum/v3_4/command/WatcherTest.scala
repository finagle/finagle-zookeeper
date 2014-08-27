package com.twitter.finagle.exp.zookeeper.integration.quorum.v3_4.command

import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.CreateMode
import com.twitter.finagle.exp.zookeeper.data.Ids
import com.twitter.finagle.exp.zookeeper.integration.quorum.QuorumIntegrationConfig
import com.twitter.finagle.exp.zookeeper.watcher.Watch
import com.twitter.util.Await
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class WatcherTest extends FunSuite with QuorumIntegrationConfig {
  test("Basic watcher") {
    newClients()
    connectClients()

    val res = for {
      _ <- client1.get.create("/zookeeper/test", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
      exist <- client2.get.exists("/zookeeper/test", true)
      setdata <- client3.get.setData("/zookeeper/test", "CHANGE IS GOOD1".getBytes, -1)
    } yield (exist, setdata)

    val (exists, setData) = Await.result(res)
    Await.result(exists.watcher.get.event)

    exists.watcher.get.event onSuccess { rep =>
      assert(rep.typ === Watch.EventType.NODE_DATA_CHANGED)
      assert(rep.state === Watch.EventState.SYNC_CONNECTED)
      assert(rep.path === "/zookeeper/test")
    }

    disconnectClients()
    closeServices()
  }

  test("Create, getData with watches , SetData") {
    newClients()
    connectClients()

    val res = for {
      _ <- client1.get.create("/zookeeper/test", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
      _ <- client3.get.sync("/zookeeper/test")
      getdata <- client3.get.getData("/zookeeper/test", true)
      _ <- client2.get.setData("/zookeeper/test", "CHANGE IS GOOD1".getBytes, -1)
    } yield getdata


    val ret = Await.result(res)
    Await.result(ret.watcher.get.event)
    ret.watcher.get.event onSuccess { rep =>
      assert(rep.typ === Watch.EventType.NODE_DATA_CHANGED)
      assert(rep.state === Watch.EventState.SYNC_CONNECTED)
      assert(rep.path === "/zookeeper/test")
    }

    disconnectClients()
    closeServices()
  }

  test("Create, getChildren with watches , delete child") {
    newClients()
    connectClients()

    val res = for {
      _ <- client3.get.create("/zookeeper/test", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
      _ <- client3.get.create("/zookeeper/test/hello", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
      _ <- client2.get.sync("/zookeeper/test")
      getchild <- client2.get.getChildren("/zookeeper/test", true)
      _ <- client3.get.delete("/zookeeper/test/hello", -1)
      _ <- client1.get.delete("/zookeeper/test", -1)
    } yield getchild


    val ret = Await.result(res)
    Await.result(ret.watcher.get.event)
    ret.watcher.get.event onSuccess { rep =>
      assert(rep.typ === Watch.EventType.NODE_CHILDREN_CHANGED)
      assert(rep.state === Watch.EventState.SYNC_CONNECTED)
      assert(rep.path === "/zookeeper/test")
    }

    disconnectClients()
    closeServices()
  }

  test("Create, getChildren2 with watches , delete child") {
    newClients()
    connectClients()

    val res = for {
      _ <- client2.get.create("/zookeeper/test", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
      _ <- client1.get.create("/zookeeper/test/hello", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
      _ <- client3.get.sync("/zookeeper/test")
      getchild <- client3.get.getChildren2("/zookeeper/test", true)
      _ <- client1.get.delete("/zookeeper/test/hello", -1)
      _ <- client1.get.delete("/zookeeper/test", -1)
    } yield getchild


    val ret = Await.result(res)
    Await.result(ret.watcher.get.event)
    ret.watcher.get.event onSuccess { rep =>
      assert(rep.typ === Watch.EventType.NODE_CHILDREN_CHANGED)
      assert(rep.state === Watch.EventState.SYNC_CONNECTED)
      assert(rep.path === "/zookeeper/test")
    }

    disconnectClients()
    closeServices()
  }

  test("Create, getChildren(watcher on parent) exists(watcher on child) , delete child") {
    newClients()
    connectClients()

    val res = for {
      _ <- client1.get.create("/zookeeper/test", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
      _ <- client2.get.create("/zookeeper/test/hello", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
      _ <- client3.get.create("/zookeeper/test/hella", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
      _ <- client2.get.sync("/zookeeper/test")
      getchild <- client2.get.getChildren("/zookeeper/test", true)
      exist <- client1.get.exists("/zookeeper/test/hello", true)
      _ <- client2.get.sync("/zookeeper/test/hella")
      getdata <- client2.get.getData("/zookeeper/test/hella", true)
      _ <- client3.get.delete("/zookeeper/test/hello", -1)
      _ <- client1.get.sync("/zookeeper/test")
      _ <- client1.get.getChildren("/zookeeper/test", true)
      _ <- client2.get.delete("/zookeeper/test/hella", -1)
      _ <- client1.get.delete("/zookeeper/test", -1)
    } yield (getchild, exist)


    val (getChildrenRep, existsRep) = Await.result(res)

    Await.result(existsRep.watcher.get.event)
    existsRep.watcher.get.event onSuccess { rep =>
      assert(rep.typ === Watch.EventType.NODE_DELETED)
      assert(rep.state === Watch.EventState.SYNC_CONNECTED)
      assert(rep.path === "/zookeeper/test/hello")
    }

    Await.result(getChildrenRep.watcher.get.event)
    getChildrenRep.watcher.get.event onSuccess { rep =>
      assert(rep.typ === Watch.EventType.NODE_CHILDREN_CHANGED)
      assert(rep.state === Watch.EventState.SYNC_CONNECTED)
      assert(rep.path === "/zookeeper/test")
    }

    disconnectClients()
    closeServices()
  }
}