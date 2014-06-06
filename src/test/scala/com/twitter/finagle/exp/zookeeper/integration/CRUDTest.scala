package com.twitter.finagle.exp.zookeeper.integration

import org.scalatest.FunSuite
import com.twitter.util.{Future, Await}
import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.CreateMode
import com.twitter.finagle.exp.zookeeper.data.{ACL, Ids}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.twitter.finagle.exp.zookeeper.data.ACL.Perms
import com.twitter.finagle.exp.zookeeper.{NodeWithWatch, NodeExistsException, NoNodeException}

@RunWith(classOf[JUnitRunner])
class CRUDTest extends FunSuite with IntegrationConfig {
  /* Configure your server here */
  val ipAddress: String = "127.0.0.1"
  val port: Int = 2181
  val timeOut: Long = 1000

  test("Server is up") {
    assert(isPortAvailable === false)
  }

  test("Client connection") {
    newClient()
    val connect = client.get.connect(2000)
    Await.ready(connect)

    Thread.sleep(500)

    val disconnect = client.get.closeSession()
    Await.ready(disconnect)
  }

  test("Node creation and exists") {
    newClient()
    connect()

    val res = for {
      _ <- client.get.create("/zookeeper/hello", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
      ret <- client.get.exists("/zookeeper/hello", watch = false)
    } yield ret

    val rep = Await.result(res)

    rep match {
      case rep: NodeWithWatch => assert(rep.stat.dataLength === "HELLO".getBytes.length)
    }

    disconnect()
    Await.ready(client.get.closeService)
  }

  test("Create, SetData, GetData, Exists, Sync") {
    newClient()
    connect()

    val res = for {
      _ <- client.get.create("/zookeeper/test", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
      exi <- client.get.exists("/zookeeper/test", watch = false)
      set <- client.get.setData("/zookeeper/test", "CHANGE IS GOOD1".getBytes, -1)
      get <- client.get.getData("/zookeeper/test", watch = false)
      sync <- client.get.sync("/zookeeper")
    } yield (exi, set, get, sync)

    val ret = Await.result(res)
    ret._1 match {
      case rep: NodeWithWatch => assert(rep.stat.dataLength === "HELLO".getBytes.length)
    }
    assert(ret._2.stat.dataLength === "CHANGE IS GOOD1".getBytes.length)
    assert(ret._3.data === "CHANGE IS GOOD1".getBytes)
    assert(ret._4.path === "/zookeeper")

    disconnect()
    Await.ready(client.get.closeService)
  }


  test("Create,SetACL, GetACL, SetData") {
    newClient()
    connect()

    val res = for {
      _ <- client.get.create("/zookeeper/test", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
      setacl <- client.get.setACL("/zookeeper/test", Ids.OPEN_ACL_UNSAFE, -1)
      getacl <- client.get.getACL("/zookeeper/test")
      _ <- client.get.setData("/zookeeper/test", "CHANGE IS GOOD1".getBytes, -1)
    } yield (setacl, getacl)

    val ret = Await.result(res)
    assert(ret._2.acl.contains(ACL(Perms.ALL, "world", "anyone")))

    disconnect()
    Await.result(client.get.closeService)
  }

  test("Create Ephemeral and Persistent Nodes") {
    newClient()
    connect()

    val res = for {
      c1 <- client.get.create("/zookeeper/ephemeralNode", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
      c2 <- client.get.create("/zookeeper/persistentNode", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
    } yield (c1, c2)

    val ret = Await.result(res)
    assert(ret._1 === "/zookeeper/ephemeralNode")
    assert(ret._2 === "/zookeeper/persistentNode")

    disconnect()
    Await.result(client.get.closeService)
  }

  test("Create 2 same ephemeral nodes should throw an exception") {
    newClient()
    connect()

    intercept[NodeExistsException] {
      val res = for {
        c1 <- client.get.create("/zookeeper/ephemeralNode", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
        c2 <- client.get.create("/zookeeper/ephemeralNode", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
      } yield (c1, c2)

      Await.result(res)
    }

    disconnect()
    Await.result(client.get.closeService)
  }

  test("Ephemeral node should not exists") {
    newClient()
    connect()

    intercept[NoNodeException] {
      val res = for {
        exi <- client.get.exists("/zookeeper/ephemeralNode", watch = false)
      } yield exi


      Await.result(res)
    }

    disconnect()
    Await.result(client.get.closeService)
  }

  test("Persistent node should still exists") {
    newClient()
    connect()

    val futu = for {
      exi <- client.get.exists("/zookeeper/persistentNode", watch = false)
    } yield exi

    val ret = Await.result(futu)

    ret match {
      case rep: NodeWithWatch => assert(rep.stat.numChildren === 0)
    }

    disconnect()
    Await.result(client.get.closeService)
  }

  test("Update persistent node") {
    newClient()
    connect()

    val res = for {
      set <- client.get.setData("/zookeeper/persistentNode", "CHANGE IS GOOD1".getBytes, -1)
    } yield set

    val ret = Await.result(res)
    assert(ret.stat.dataLength === "CHANGE IS GOOD1".getBytes.length)

    disconnect()
    Await.result(client.get.closeService)
  }

  test("Add Children to persistent node") {
    newClient()
    connect()

    val res = for {
      c1 <- client.get.create("/zookeeper/persistentNode/firstChild", "HELLO-FIRSST".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
      c2 <- client.get.create("/zookeeper/persistentNode/secondChild", "HELLO-SECOOND".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
    } yield (c1, c2)

    val ret = Await.result(res)

    assert(ret._1 === "/zookeeper/persistentNode/firstChild")
    assert(ret._2 === "/zookeeper/persistentNode/secondChild")

    disconnect()
    Await.result(client.get.closeService)
  }

  test("Add 8 sequential nodes") {
    newClient()
    connect()

    val res = for {
      _ <- client.get.create("/zookeeper/persistentNode/sequentialNode-", "SEQ".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL)
      _ <- client.get.create("/zookeeper/persistentNode/sequentialNode-", "SEQ".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL)
      _ <- client.get.create("/zookeeper/persistentNode/sequentialNode-", "SEQ".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL)
      _ <- client.get.create("/zookeeper/persistentNode/sequentialNode-", "SEQ".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL)
      _ <- client.get.create("/zookeeper/persistentNode/sequentialNode-", "SEQ".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL)
      _ <- client.get.create("/zookeeper/persistentNode/sequentialNode-", "SEQ".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL)
      _ <- client.get.create("/zookeeper/persistentNode/sequentialNode-", "SEQ".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL)
      _ <- client.get.create("/zookeeper/persistentNode/sequentialNode-", "SEQ".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL)
    } yield None

    Await.ready(res)

    disconnect()
    Await.result(client.get.closeService)
  }

  test("GetChildren and GetChildren2 on persistent node") {
    newClient()
    connect()

    val res = for {
      c1 <- client.get.getChildren("/zookeeper/persistentNode", watch = false)
      c2 <- client.get.getChildren2("/zookeeper/persistentNode", watch = false)
    } yield (c1, c2)

    val ret = Await.result(res)

    assert(ret._1.children.size === 10)
    assert(ret._2.stat.numChildren === 10)

    disconnect()
    Await.result(client.get.closeService)
  }

  test("Delete persistent node and children") {
    newClient()
    connect()

    val ret = for {
      children <- client.get.getChildren("/zookeeper/persistentNode", watch = false)
    } yield children


    val f = ret.flatMap { response =>
      response.children foreach (child => client.get.delete("/zookeeper/persistentNode/" + child, -1))
      Future(response)
    }

    Await.ready(f)
    val deleteNode = client.get.delete("/zookeeper/persistentNode", -1)
    Await.result(deleteNode)

    disconnect()
    Await.result(client.get.closeService)
  }

  test("Persistent node and children should not exist") {
    newClient()
    connect()

    intercept[NoNodeException] {
      val res = for {
        exi <- client.get.exists("/zookeeper/persistentNode", watch = false)
      } yield exi

      val ret = Await.result(res)
    }

    disconnect()
    Await.result(client.get.closeService)
  }

}
