package com.twitter.finagle.exp.zookeeper.integration

import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.CreateMode
import com.twitter.finagle.exp.zookeeper.data.Ids
import com.twitter.finagle.exp.zookeeper.watcher.Watch
import com.twitter.util.Await
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class WatchTest extends IntegrationConfig {
  /* Configure your server here */
  val ipAddress: String = "127.0.0.1"
  val port: Int = 2181
  val timeOut: Long = 1000

  test("Server is up") {
    assert(isPortAvailable === false)
  }

  test("Create, exists with watches , SetData") {
    newClient()
    connect()

    val res = for {
      _ <- client.get.create("/zookeeper/test", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
      exist <- client.get.exists("/zookeeper/test", true)
      setdata <- client.get.setData("/zookeeper/test", "CHANGE IS GOOD1".getBytes, -1)
    } yield (exist, setdata)

    val (exists, setData) = Await.result(res)
    Await.result(exists.watcher.get.event)

    exists.watcher.get.event onSuccess { rep =>
      assert(rep.typ === Watch.EventType.NODE_DATA_CHANGED)
      assert(rep.state === Watch.EventState.SYNC_CONNECTED)
      assert(rep.path === "/zookeeper/test")
    }

    disconnect()
    Await.ready(client.get.closeService())
  }

  test("Create, getData with watches , SetData") {
    newClient()
    connect()

    val res = for {
      _ <- client.get.create("/zookeeper/test", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
      getdata <- client.get.getData("/zookeeper/test", true)
      _ <- client.get.setData("/zookeeper/test", "CHANGE IS GOOD1".getBytes, -1)
    } yield getdata


    val ret = Await.result(res)
    Await.result(ret.watcher.get.event)
    ret.watcher.get.event onSuccess { rep =>
      assert(rep.typ === Watch.EventType.NODE_DATA_CHANGED)
      assert(rep.state === Watch.EventState.SYNC_CONNECTED)
      assert(rep.path === "/zookeeper/test")
    }

    disconnect()
    Await.ready(client.get.closeService())
  }

  test("Create, getChildren with watches , delete child") {
    newClient()
    connect()

    val res = for {
      _ <- client.get.create("/zookeeper/test", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
      _ <- client.get.create("/zookeeper/test/hello", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
      getchild <- client.get.getChildren("/zookeeper/test", true)
      _ <- client.get.delete("/zookeeper/test/hello", -1)
      _ <- client.get.delete("/zookeeper/test", -1)
    } yield getchild


    val ret = Await.result(res)
    Await.result(ret.watcher.get.event)
    ret.watcher.get.event onSuccess { rep =>
      assert(rep.typ === Watch.EventType.NODE_CHILDREN_CHANGED)
      assert(rep.state === Watch.EventState.SYNC_CONNECTED)
      assert(rep.path === "/zookeeper/test")
    }

    disconnect()
    Await.ready(client.get.closeService())
  }

  test("Create, getChildren2 with watches , delete child") {
    newClient()
    connect()

    val res = for {
      _ <- client.get.create("/zookeeper/test", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
      _ <- client.get.create("/zookeeper/test/hello", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
      getchild <- client.get.getChildren2("/zookeeper/test", true)
      _ <- client.get.delete("/zookeeper/test/hello", -1)
      _ <- client.get.delete("/zookeeper/test", -1)
    } yield getchild


    val ret = Await.result(res)
    Await.result(ret.watcher.get.event)
    ret.watcher.get.event onSuccess { rep =>
      assert(rep.typ === Watch.EventType.NODE_CHILDREN_CHANGED)
      assert(rep.state === Watch.EventState.SYNC_CONNECTED)
      assert(rep.path === "/zookeeper/test")
    }

    disconnect()
    Await.ready(client.get.closeService())
  }

  test("Create, getChildren(watcher on parent) exists(watcher on child) , delete child") {
    newClient()
    connect()

    val res = for {
      _ <- client.get.create("/zookeeper/test", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
      _ <- client.get.create("/zookeeper/test/hello", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
      _ <- client.get.create("/zookeeper/test/hella", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
      getchild <- client.get.getChildren("/zookeeper/test", true)
      exist <- client.get.exists("/zookeeper/test/hello", true)
      getdata <- client.get.getData("/zookeeper/test/hella", true)
      _ <- client.get.delete("/zookeeper/test/hello", -1)
      _ <- client.get.getChildren("/zookeeper/test", true)
      _ <- client.get.delete("/zookeeper/test/hella", -1)
      _ <- client.get.delete("/zookeeper/test", -1)
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

    disconnect()
    Await.ready(client.get.closeService())
  }


}
