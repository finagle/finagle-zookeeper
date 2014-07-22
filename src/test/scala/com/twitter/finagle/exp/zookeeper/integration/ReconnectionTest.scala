package com.twitter.finagle.exp.zookeeper.integration

import com.twitter.finagle.exp.zookeeper.{ExistsResponse, AuthFailedException}
import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.CreateMode
import com.twitter.finagle.exp.zookeeper.data.{Stat, Auth, Ids}
import com.twitter.util.Await
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ReconnectionTest extends FunSuite with IntegrationConfig {
  test("Client connection") {
    newClient()
    connect()

    Await.result(client.get.changeHost())
    Await.result(client.get.changeHost(Some("127.0.0.1:2181")))

    val res = for {
      _ <- client.get.create("/zookeeper/test", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
      exi <- client.get.exists("/zookeeper/test", watch = false)
      set <- client.get.setData("/zookeeper/test", "CHANGE IS GOOD1".getBytes, -1)
      get <- client.get.getData("/zookeeper/test", watch = false)
      sync <- client.get.sync("/zookeeper")
    } yield (exi, set, get, sync)

    val ret = Await.result(res)
    ret._1 match {
      case rep: ExistsResponse => assert(rep.stat.get.dataLength === "HELLO".getBytes.length)
      case _ => throw new RuntimeException("Test failed")
    }
    assert(ret._2.dataLength === "CHANGE IS GOOD1".getBytes.length)
    assert(ret._3.data === "CHANGE IS GOOD1".getBytes)
    assert(ret._4 === "/zookeeper")

    disconnect()
    Await.ready(client.get.closeService())
  }

  test("connect-disconnect tests") {
    newClient()

    for (i <- 0 until 50) {
      connect()
      disconnect()
    }

    Await.ready(client.get.closeService())
  }

  test("auth failed reconnection") {
    newClient()
    connect()

    intercept[AuthFailedException] {
      Await.result(client.get.addAuth(Auth("FOO", "BAR".getBytes)))
    }

    val rep = Await.result {
      client.get.create("/hello", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
    }

    assert(rep === "/hello")

    val disconnect = client.get.closeSession() before client.get.closeService()
    Await.ready(disconnect)
  }
}
