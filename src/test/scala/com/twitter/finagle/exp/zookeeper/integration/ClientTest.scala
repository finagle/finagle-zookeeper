package com.twitter.finagle.exp.zookeeper.integration

import org.scalatest.FunSuite
import com.twitter.util.{Future, Await}
import com.twitter.finagle.exp.zookeeper._
import com.twitter.finagle.exp.zookeeper.ZookeeperDefinitions.createMode

class ClientTest extends FunSuite with IntegrationConfig {
  /* Configure your server here */
  val ipAddress: String = "127.0.0.1"
  val port: Int = 2181
  val timeOut: Long = 1000

  /**
   * Note : The tests are working separately but no together
   * this behaviour may be related to the serialization issues
   * when not using a for comprehension to process requests
   */

  test("Server is up") {
    assert(isPortAvailable === false)
  }

  test("Client connection") {
    val connect = client.get.connect()
    connect onSuccess {
      a =>
        Thread.sleep(15000)
        Await.result(client.get.closeSession)
        assert(true)
    } onFailure { exc =>
      println(exc.getCause)
    }
  }

  test("Connection test") {
    connect

    Thread.sleep(10000)

    disconnect
  }

  test("Node creation and exists") {
    val connect = client.get.connect()
    Await.ready(connect)

    val res = for {
      _ <- client.get.create("/zookeeper/hello", "HELLO".getBytes, ACL.defaultACL, createMode.EPHEMERAL)
      ret <- client.get.exists("/zookeeper/hello", false)
    } yield ret

    val rep = Await.result(res)

    assert(rep.stat.dataLength === "HELLO".getBytes.length)

    disconnect
  }

  test("Create, SetData, GetData, Exists, Sync") {
    connect

    val res = for {
      _ <- client.get.create("/zookeeper/test", "HELLO".getBytes, ACL.defaultACL, createMode.EPHEMERAL)
      exi <- client.get.exists("/zookeeper/test", false)
      set <- client.get.setData("/zookeeper/test", "CHANGE IS GOOD1".getBytes, -1)
      get <- client.get.getData("/zookeeper/test", false)
      sync <- client.get.sync("/zookeeper")
    } yield (exi, set, get, sync)

    val ret = Await.result(res)
    assert(ret._1.stat.dataLength === "HELLO".getBytes.length)
    assert(ret._2.stat.dataLength === "CHANGE IS GOOD1".getBytes.length)
    assert(ret._3.data === "CHANGE IS GOOD1".getBytes)
    assert(ret._4.path === "/zookeeper")

    disconnect
  }
  test("Create, exists with watches , SetData") {
    connect

    val res = for {
      _ <- client.get.create("/zookeeper/test", "HELLO".getBytes, ACL.defaultACL, createMode.EPHEMERAL)
      exi <- client.get.exists("/zookeeper/test", true)
      set <- client.get.setData("/zookeeper/test", "CHANGE IS GOOD1".getBytes, -1)
    } yield (exi, set)

    val ret = Await.result(res)
    //assert(ret._2.acl.contains(ACL(Perms.ALL, "world", "anyone")))

    disconnect
  }

  test("Create,SetACL, GetACL, SetData") {
    connect

    val res = for {
      _ <- client.get.create("/zookeeper/test", "HELLO".getBytes, ACL.defaultACL, createMode.EPHEMERAL)
      setacl <- client.get.setACL("/zookeeper/test", Array(ACL(Perms.ALL, "world", "anyone")), -1)
      getacl <- client.get.getACL("/zookeeper/test")
      _ <- client.get.setData("/zookeeper/test", "CHANGE IS GOOD1".getBytes, -1)
    } yield (setacl, getacl)

    val ret = Await.result(res)
    assert(ret._2.acl.contains(ACL(Perms.ALL, "world", "anyone")))

    disconnect
  }

  test("Create Ephemeral and Persistent Nodes") {
    connect

    val res = for {
      c1 <- client.get.create("/zookeeper/ephemeralNode", "HELLO".getBytes, ACL.defaultACL, createMode.EPHEMERAL)
      c2 <- client.get.create("/zookeeper/persistentNode", "HELLO".getBytes, ACL.defaultACL, createMode.PERSISTENT)
    } yield (c1, c2)

    val ret = Await.result(res)
    assert(ret._1.path === "/zookeeper/ephemeralNode")
    assert(ret._2.path === "/zookeeper/persistentNode")
    disconnect
  }

  test("Ephemeral node should not exists") {
    connect

    intercept[NoNodeException] {
      val res = for {
        exi <- client.get.exists("/zookeeper/ephemeralNode", false)
      } yield exi


      val ret = Await.result(res)
    }

    disconnect
  }

  test("Persistent node should still exists") {
    connect

    val futu = for {
      exi <- client.get.exists("/zookeeper/persistentNode", false)
    } yield (exi)

    val ret = Await.result(futu)
    assert(ret.stat.numChildren === 0)

    disconnect
  }

  test("Update persistent node") {
    connect

    val res = for {
      set <- client.get.setData("/zookeeper/persistentNode", "CHANGE IS GOOD1".getBytes, -1)
    } yield (set)

    val ret = Await.result(res)
    assert(ret.stat.dataLength === "CHANGE IS GOOD1".getBytes.length)

    disconnect
  }

  test("Add Children to persistent node") {
    connect

    val res = for {
      c1 <- client.get.create("/zookeeper/persistentNode/firstChild", "HELLO-FIRSST".getBytes, ACL.defaultACL, createMode.PERSISTENT)
      c2 <- client.get.create("/zookeeper/persistentNode/secondChild", "HELLO-SECOOND".getBytes, ACL.defaultACL, createMode.PERSISTENT)
    } yield (c1, c2)

    val ret = Await.result(res)

    assert(ret._1.path === "/zookeeper/persistentNode/firstChild")
    assert(ret._2.path === "/zookeeper/persistentNode/secondChild")

    disconnect
  }

  test("Add 8 sequential nodes") {
    connect

    val res = for {
      _ <- client.get.create("/zookeeper/persistentNode/sequentialNode-", "SEQ".getBytes, ACL.defaultACL, createMode.PERSISTENT_SEQUENTIAL)
      _ <- client.get.create("/zookeeper/persistentNode/sequentialNode-", "SEQ".getBytes, ACL.defaultACL, createMode.PERSISTENT_SEQUENTIAL)
      _ <- client.get.create("/zookeeper/persistentNode/sequentialNode-", "SEQ".getBytes, ACL.defaultACL, createMode.PERSISTENT_SEQUENTIAL)
      _ <- client.get.create("/zookeeper/persistentNode/sequentialNode-", "SEQ".getBytes, ACL.defaultACL, createMode.PERSISTENT_SEQUENTIAL)
      _ <- client.get.create("/zookeeper/persistentNode/sequentialNode-", "SEQ".getBytes, ACL.defaultACL, createMode.PERSISTENT_SEQUENTIAL)
      _ <- client.get.create("/zookeeper/persistentNode/sequentialNode-", "SEQ".getBytes, ACL.defaultACL, createMode.PERSISTENT_SEQUENTIAL)
      _ <- client.get.create("/zookeeper/persistentNode/sequentialNode-", "SEQ".getBytes, ACL.defaultACL, createMode.PERSISTENT_SEQUENTIAL)
      _ <- client.get.create("/zookeeper/persistentNode/sequentialNode-", "SEQ".getBytes, ACL.defaultACL, createMode.PERSISTENT_SEQUENTIAL)
    } yield (None)


    val ret = Await.result(res)

    disconnect
  }

  test("GetChildren and GetChildren2 on persistent node") {
    connect

    val res = for {
      c1 <- client.get.getChildren("/zookeeper/persistentNode", false)
      c2 <- client.get.getChildren2("/zookeeper/persistentNode", false)
    } yield (c1, c2)

    val ret = Await.result(res)

    assert(ret._1.children.size === 10)
    assert(ret._2.stat.numChildren === 10)

    disconnect
  }

  test("Delete persistent node and children") {
    connect

    val ret = for {
      children <- client.get.getChildren("/zookeeper/persistentNode", false)
    } yield (children)


    val f = ret.flatMap { response =>
      response.children foreach (child => client.get.delete("/zookeeper/persistentNode/" + child, -1))
      Future(response)
    }

    Await.ready(f)
    val deleteNode = client.get.delete("/zookeeper/persistentNode", -1)
    Await.result(deleteNode)
    disconnect
  }

  test("Persistent node and children should not exist") {
    connect

    intercept[NoNodeException] {
      val res = for {
        exi <- client.get.exists("/zookeeper/persistentNode", false)
      } yield exi

      val ret = Await.result(res)
    }

    disconnect
  }

  test("Small create Transaction works") {
    connect

    val opList = Array[OpRequest](new CreateOp("/zookeeper/hello", "TRANS".getBytes(), ACL.defaultACL, createMode.EPHEMERAL),
      new CreateOp("/zookeeper/world", "TRANS".getBytes(), ACL.defaultACL, createMode.EPHEMERAL))

    val res = client.get.transaction(opList)
    val fut = Await.result(res)

    assert(fut.responseList(0).asInstanceOf[CreateResult].path === "/zookeeper/hello")
    assert(fut.responseList(1).asInstanceOf[CreateResult].path === "/zookeeper/world")


    disconnect
  }
}
