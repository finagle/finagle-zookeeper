package com.twitter.finagle.exp.zookeeper.integration.standalone.command

import com.twitter.finagle.exp.zookeeper.Zookeeper
import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.CreateMode
import com.twitter.finagle.exp.zookeeper.data.Ids
import com.twitter.finagle.exp.zookeeper.integration.standalone.StandaloneIntegrationConfig
import com.twitter.finagle.exp.zookeeper.watcher.Watch
import com.twitter.finagle.exp.zookeeper.watcher.Watch.{EventState, EventType}
import com.twitter.util.{Await, Future}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class WatcherTest extends StandaloneIntegrationConfig {
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
    Await.ready(client.get.close())
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
    Await.ready(client.get.close())
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
    Await.ready(client.get.close())
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
    Await.ready(client.get.close())
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
    Await.ready(client.get.close())
  }

  test("complete watcher test") {
    newClient()
    connect()

    val ret = Await.result {
      Future.collect {
        0 until 100 map { i =>
          for {
            _ <- client.get.create(
              "/foo-" + i, ("foodata" + i).getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT
            )
            getdata <- client.get.getData("/foo-" + i, true)
            exists <- client.get.exists("/foo-" + i, true)
            setdata <- client.get.setData("/foo-" + i, ("foodata2-" + i).getBytes, -1)
            setdata2 <- client.get.setData("/foo-" + i, ("foodata3-" + i).getBytes, -1)
          } yield (getdata, exists, setdata, setdata2)
        }
      }
    }

    ret map { grpRep =>
      val (getdata, exists, stat, stat2) = grpRep
      assert(getdata.watcher.get.event.isDefined)
      assert(Await.result(getdata.watcher.get.event).typ === EventType.NODE_DATA_CHANGED)
      assert(Await.result(getdata.watcher.get.event).state === EventState.SYNC_CONNECTED)
      assert(exists.watcher.get.event.isDefined)
      assert(Await.result(exists.watcher.get.event).typ === EventType.NODE_DATA_CHANGED)
      assert(Await.result(exists.watcher.get.event).state === EventState.SYNC_CONNECTED)
    }

    Await.result {
      Future.collect {
        0 until 100 map { i =>
          for {
            _ <- client.get.delete("/foo-" + i, -1)
          } yield None
        }
      }
    }

    disconnect()
    Await.result(client.get.close())
  }

  test("auto reset with chroot") {
    newClient()
    connect()

    Await.ready {
      for {
        _ <- client.get.create("/ch1", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
      } yield None
    }

    val client2 = Zookeeper
      .withZkConfiguration(true, true, "/ch1")
      .newRichClient(ipAddress + ":" + port)

    Await.ready(client2.connect())
    val watcherF = Await.result(client2.getChildren("/", true)).watcher.get

    Await.ready {
      for {
        _ <- client.get.create(
          "/youdontmatter", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT
        )
        _ <- client.get.create(
          "/ch1/youdontmatter", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT
        )
      } yield None
    }

    val watcher = Await.result(watcherF.event)
    assert(watcher.typ === EventType.NODE_CHILDREN_CHANGED)
    assert(watcher.path === "/")

    Await.ready {
      for {
        _ <- client.get.delete("/ch1/youdontmatter", -1)
        _ <- client.get.delete("/ch1", -1)
        _ <- client.get.delete("/youdontmatter", -1)
      } yield None
    }

    Await.ready(client2.disconnect())
    Await.ready(client2.close())
    disconnect()
    Await.result(client.get.close())
  }

  test("deep auto reset with chroot") {
    newClient()
    connect()

    Await.ready {
      for {
        _ <- client.get.create("/ch1", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
        _ <- client.get.create("/ch1/here", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
        _ <- client.get.create("/ch1/here/we", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
        _ <- client.get.create("/ch1/here/we/are", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
      } yield None
    }

    val client2 = Zookeeper
      .withZkConfiguration(true, true, "/ch1/here/we")
      .newRichClient(ipAddress + ":" + port)

    Await.ready(client2.connect())
    val watcherF = Await.result(client2.getChildren("/are", true)).watcher.get

    Await.ready {
      client.get.create(
        "/ch1/here/we/are/now", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT
      )
    }

    val watcher = Await.result(watcherF.event)
    assert(watcher.typ === Watch.EventType.NODE_CHILDREN_CHANGED)
    assert(watcher.path === "/are")
    assert(watcherF.path === "/are")

    Await.ready {
      for {
        _ <- client.get.delete("/ch1/here/we/are/now", -1)
        _ <- client.get.delete("/ch1/here/we/are", -1)
        _ <- client.get.delete("/ch1/here/we", -1)
        _ <- client.get.delete("/ch1/here", -1)
        _ <- client.get.delete("/ch1", -1)
      } yield None
    }

    Await.ready(client2.disconnect())
    Await.ready(client2.close())
    disconnect()
    Await.result(client.get.close())
  }
}
