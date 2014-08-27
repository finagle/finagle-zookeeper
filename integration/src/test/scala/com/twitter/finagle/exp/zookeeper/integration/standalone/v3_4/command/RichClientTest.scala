package com.twitter.finagle.exp.zookeeper.integration.standalone.v3_4.command

import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.CreateMode
import com.twitter.finagle.exp.zookeeper._
import com.twitter.finagle.exp.zookeeper.data.Ids
import com.twitter.finagle.exp.zookeeper.integration.standalone.StandaloneIntegrationConfig
import com.twitter.finagle.exp.zookeeper.watcher.Watch.{EventState, EventType}
import com.twitter.util.{Await, Future}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RichClientTest extends FunSuite with StandaloneIntegrationConfig {
  test("async test") {
    newClient()
    connect()

    intercept[NoAuthException] {
      Await.result {
        client.get.addAuth("digest", "ben:passwd".getBytes) before
          client.get.create("/ben", "".getBytes, Ids.READ_ACL_UNSAFE, CreateMode.PERSISTENT).unit before
          client.get.create("/ben/2", "".getBytes, Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT)
      }
    }

    Await.result {
      for {
        _ <- client.get.delete("/ben", -1)
        _ <- client.get.create("/ben2", "".getBytes, Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT)
        _ <- client.get.getData("/ben2")
      } yield None
    }

    disconnect()
    Await.ready(client.get.close())
    newClient()
    connect()

    intercept[NoAuthException] {
      Await.result {
        client.get.addAuth("digest", "ben:passwd2".getBytes) before
          client.get.getData("/ben2")
      }
    }

    disconnect()
    Await.ready(client.get.close())
    newClient()
    connect()

    Await.result {
      client.get.addAuth("digest", "ben:passwd".getBytes) before
        client.get.getData("/ben2")
    }

    Await.result {
      client.get.delete("/ben2", -1)
    }

    disconnect()
    Await.ready(client.get.close())
  }

  test("rich client test") {
    newClient()
    connect()

    Await.result {
      client.get.create(
        "/benwashere", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT
      )
    }

    intercept[BadVersionException] {
      Await.result {
        client.get.setData("/benwashere", "hi".getBytes, 57)
      }
    }

    Await.result {
      client.get.delete("/benwashere", 0)
    }

    disconnect()
    newClient()
    connect()

    intercept[BadArgumentsException] {
      Await.result {
        client.get.delete("/", -1)
      }
    }

    val rep = Await.result {
      for {
        _ <- client.get.create(
          "/pat", "pat was here".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT
        )
        _ <- client.get.create(
          "/pat/ben", "ben was here".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT
        )
        children <- client.get.getChildren("/pat")
      } yield children
    }

    assert(rep.children.size === 1)
    assert(rep.children(0) === "ben")

    val rep2 = Await.result {
      client.get.getChildren2("/pat")
    }

    assert(rep2.children === rep.children)

    // Test stat and watch of non existent node
    val exists = Await.result {
      for {
        exists <- client.get.exists("/frog", true)
        _ <- client.get.create(
          "/frog", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT
        )
        _ <- client.get.delete("/frog", -1)
      } yield exists
    }

    assert(Await.result(exists.watcher.get.event).typ === EventType.NODE_CREATED)
    assert(Await.result(exists.watcher.get.event).state === EventState.SYNC_CONNECTED)

    // Test child watch and create with sequence
    val childrenWatch = Await.result {
      client.get.getChildren("/pat/ben", true)
    }

    val ret = Await.result {
      Future.collect {
        0 until 10 map { i =>
          for {
            _ <- client.get.create(
              "/pat/ben/" + i + "-",
              ("" + i).getBytes,
              Ids.OPEN_ACL_UNSAFE,
              CreateMode.PERSISTENT_SEQUENTIAL
            )
          } yield None
        }
      }
    }

    val children = Await.result {
      client.get.getChildren("/pat/ben")
    }

    assert(children.children.size === 10)
    children.children map { child =>
      val rep = Await.result {
        for {
          getdata <- client.get.getData("/pat/ben/" + child, true)
          setdata <- client.get.setData("/pat/ben/" + child, "new".getBytes, -1)
          exist <- client.get.exists("/pat/ben/" + child, true)
          _ <- client.get.delete("/pat/ben/" + child, -1)
        } yield (getdata, exist)
      }

      val (getdata, exist) = rep
      assert(Await.result(getdata.watcher.get.event).typ === EventType.NODE_DATA_CHANGED)
      assert(Await.result(exist.watcher.get.event).typ === EventType.NODE_DELETED)
    }

    assert(Await.result(childrenWatch.watcher.get.event).typ === EventType.NODE_CHILDREN_CHANGED)

    Await.result {
      client.get.delete("/pat/ben", -1) before
        client.get.delete("/pat", -1)
    }

    disconnect()
    Await.ready(client.get.close())
  }

  test("large node") {
    newClient()
    connect()

    Await.result {
      client.get.create(
        "/large", new Array[Byte](500000), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL
      )
    }

    disconnect()
    Await.result(client.get.close())
  }

  test("create fails") {
    newClient()
    connect()

    def createFails(path: String) {
      intercept[Exception] {
        Await.result {
          client.get.create(
            path, "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT
          )
        }
      }
    }

    createFails("")
    createFails("//")
    createFails("///")
    createFails("////")
    createFails("/.")
    createFails("/integrationTests/src/main")
    createFails("/./")
    createFails("/integrationTests/src/main")
    createFails("/foo/./")
    createFails("/foo/../")
    createFails("/foo/.")
    createFails("/foo/..")
    createFails("/./.")
    createFails("/core/src")
    createFails("/\u0001foo")
    createFails("/foo/bar/")
    createFails("/foo//bar")
    createFails("/foo/bar//")
    createFails("foo")
    createFails("a")

    disconnect()
    Await.result(client.get.close())
  }

  test("delete with children") {
    newClient()
    connect()

    Await.result {
      client.get.create("/parent", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
      client.get.create("/parent/child", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
    }

    intercept[NotEmptyException] {
      Await.result {
        client.get.delete("/parent", -1)
      }
    }

    Await.ready {
      client.get.delete("/parent/child", -1) before
        client.get.delete("/parent", -1)
    }

    disconnect()
    Await.result(client.get.close())
  }

  test("getChildren test 1") {
    newClient()
    connect()

    Await.ready {
      client.get.create("/foo", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
      client.get.create("/foo/bar", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
    }

    val rep = Await.result {
      client.get.getChildren2("/foo")
    }

    assert(rep.stat.czxid === rep.stat.mzxid)
    assert(rep.stat.czxid + 1 === rep.stat.pzxid)
    assert(rep.stat.ctime === rep.stat.mtime)
    assert(rep.stat.cversion === 1)
    assert(rep.stat.version === 0)
    assert(rep.stat.aversion === 0)
    assert(rep.stat.ephemeralOwner === 0L)
    assert(rep.stat.dataLength === "".length)
    assert(rep.stat.numChildren === 1)

    val rep2 = Await.result {
      client.get.getChildren2("/foo/bar")
    }

    assert(rep2.stat.czxid === rep2.stat.mzxid)
    assert(rep2.stat.czxid === rep2.stat.pzxid)
    assert(rep2.stat.ctime === rep2.stat.mtime)
    assert(rep2.stat.cversion === 0)
    assert(rep2.stat.version === 0)
    assert(rep2.stat.aversion === 0)
    assert(rep2.stat.ephemeralOwner === client.get.session.id)
    assert(rep2.stat.dataLength === "".length)
    assert(rep2.stat.numChildren === 0)

    Await.ready {
      client.get.delete("/foo/bar", -1)
      client.get.delete("/foo", -1)
    }

    disconnect()
    Await.result(client.get.close())
  }
}