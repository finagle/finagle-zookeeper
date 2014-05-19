package com.twitter.finagle.exp.zookeeper.integration

import org.scalatest.FunSuite
import java.net.{BindException, ServerSocket}
import com.twitter.finagle.exp.zookeeper.client.ClientWrapper
import com.twitter.util.{Future, Await}
import com.twitter.finagle.exp.zookeeper._
import com.twitter.finagle.exp.zookeeper.ZookeeperDefinitions.createMode
import scala.Some

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
    val connect = client.get.connect
    Await.result(connect)

    connect onSuccess {
      a =>
        Await.result(client.get.disconnect)
        assert(true)
    }
  }

  test("Node creation and exists") {
    connect

    val res = for {
      _ <- client.get.create("/zookeeper/test", "HELLO".getBytes, ACL.defaultACL, createMode.EPHEMERAL)
      ret <- client.get.exists("/zookeeper/test", false)
    } yield ret

    val rep = Await.result(res)
    assert(rep.get.stat.dataLength === "HELLO".getBytes.length)

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
    assert(ret._1.get.stat.dataLength === "HELLO".getBytes.length)
    assert(ret._2.get.stat.dataLength === "CHANGE IS GOOD1".getBytes.length)
    assert(ret._3.get.data === "CHANGE IS GOOD1".getBytes)
    assert(ret._4.get.path === "/zookeeper")

    disconnect
  }

  test("Create,SetACL, GetACL, SetData") {
    connect

    val res = for {
      _ <- client.get.create("/zookeeper/test", "HELLO".getBytes, ACL.defaultACL, createMode.EPHEMERAL)
      setacl <- client.get.setACL("/zookeeper/test", Array(ACL(Perms.ALL, "world", "anyone")), -1)
      getacl <- client.get.getACL("/zookeeper/test")
    } yield (setacl, getacl)

    val ret = Await.result(res)
    assert(ret._2.get.acl.contains(ACL(Perms.ALL, "world", "anyone")))

    disconnect
  }

  test("Create Ephemeral and Persistent Nodes") {
    connect

    val res = for {
      c1 <- client.get.create("/zookeeper/ephemeralNode", "HELLO".getBytes, ACL.defaultACL, createMode.EPHEMERAL)
      c2 <- client.get.create("/zookeeper/persistentNode", "HELLO".getBytes, ACL.defaultACL, createMode.PERSISTENT)
    } yield (c1, c2)

    val ret = Await.result(res)
    assert(ret._1.get.path === "/zookeeper/ephemeralNode")
    assert(ret._2.get.path === "/zookeeper/persistentNode")
    disconnect
  }

  test("Ephemeral node should not exists") {
    connect

    val res = for {
      exi <- client.get.exists("/zookeeper/ephemeralNode", false)
    } yield exi

    val ret = Await.result(res)
    assert(ret match {
      case None => true
      case Some(res) => false
    })

    disconnect
  }

  test("Persistent node should still exists") {
    connect

    val res = for {
      exi <- client.get.exists("/zookeeper/persistentNode", false)
    } yield (exi)

    val ret = Await.result(res)
    assert(ret match {
      case None => false
      case Some(res) => {
        assert(res.stat.dataLength === "HELLO".getBytes.length)
        true
      }
    })

    disconnect
  }

  test("Update persistent node") {
    connect

    val res = for {
      set <- client.get.setData("/zookeeper/persistentNode", "CHANGE IS GOOD1".getBytes, -1)
    } yield (set)

    val ret = Await.result(res)
    assert(ret.get.stat.dataLength === "CHANGE IS GOOD1".getBytes.length)

    disconnect
  }

  test("Add Children to persistent node") {
    connect

    val res = for {
      c1 <- client.get.create("/zookeeper/persistentNode/firstChild", "HELLO-FIRSST".getBytes, ACL.defaultACL, createMode.PERSISTENT)
      c2 <- client.get.create("/zookeeper/persistentNode/secondChild", "HELLO-SECOOND".getBytes, ACL.defaultACL, createMode.PERSISTENT)
    } yield (c1, c2)

    val ret = Await.result(res)

    assert(ret._1.get.path === "/zookeeper/persistentNode/firstChild")
    assert(ret._2.get.path === "/zookeeper/persistentNode/secondChild")

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

    assert(ret._1.get.children.size === 10)
    assert(ret._2.get.stat.numChildren === 10)

    disconnect
  }

  test("Delete persistent node and children") {
    connect

    val ret = for {
      children <- client.get.getChildren("/zookeeper/persistentNode", false)
    } yield (children)


    val f = ret.flatMap { response =>
      response.get.children foreach (child => client.get.delete("/zookeeper/persistentNode/" + child, -1))
      Future(response)
    }

    val deleteNode = client.get.delete("/zookeeper/persistentNode", -1)


    Await.ready(f)
    Await.ready(deleteNode)
    disconnect
  }

  test("Persistent node and children should not exist") {
    connect

    val res = for {
      exi <- client.get.exists("/zookeeper/persistentNode", false)
    } yield exi

    val ret = Await.result(res)
    assert(ret match {
      case None => true
      case Some(res) => false
    })

    disconnect
  }

  test("Small create Transaction works") {
    connect

    val opList = Array[OpRequest](new CreateOp("/zookeeper/hello", "TRANS".getBytes(), ACL.defaultACL, createMode.EPHEMERAL),
      new CreateOp("/zookeeper/world", "TRANS".getBytes(), ACL.defaultACL, createMode.EPHEMERAL))

    val res = client.get.transaction(opList)
    val fut = Await.result(res)

    assert(fut match {
      case None => false
      case Some(res) => {
        assert(res(0).asInstanceOf[CreateResult].path === "/zookeeper/hello")
        assert(res(1).asInstanceOf[CreateResult].path === "/zookeeper/world")
        true
      }
    })

    Thread.sleep(10000)
    disconnect
  }
}
