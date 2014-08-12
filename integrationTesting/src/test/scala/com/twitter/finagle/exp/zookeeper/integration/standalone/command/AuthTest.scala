package com.twitter.finagle.exp.zookeeper.integration.standalone.command

import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.CreateMode
import com.twitter.finagle.exp.zookeeper.data.{Auth, Ids}
import com.twitter.finagle.exp.zookeeper.integration.standalone.StandaloneIntegrationConfig
import com.twitter.finagle.exp.zookeeper.{AuthFailedException, NoAuthException}
import com.twitter.util.Await
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AuthTest extends FunSuite with StandaloneIntegrationConfig {
  test("Bad Auth Notifies Watch") {
    newClient()
    connect()

    intercept[AuthFailedException] {
      Await.result(client.get.addAuth(Auth("FOO", "BAR".getBytes)))
    }

    Await.ready(client.get.closeService())
  }

  test("Bad auth and other commands") {
    newClient()
    connect()

    intercept[AuthFailedException] {
      Await.result(client.get.addAuth(Auth("FOO", "BAR".getBytes)))
    }

    intercept[Exception] {
      Await.result(client.get.exists("/foobar"))
    }

    intercept[Exception] {
      Await.result(client.get.getData("/path1"))
    }

    Await.ready(client.get.closeService())
  }

  test("Auth super test 1") {
    newClient()
    connect()

    Await.result {
      for {
        _ <- client.get.addAuth(Auth("digest", "pat:pass".getBytes))
        _ <- client.get.create("/path1", "".getBytes, Ids.CREATOR_ALL_ACL,
          CreateMode.PERSISTENT)
      } None
    }

    disconnect()
    Await.ready(client.get.closeService())

    newClient()
    connect()

    Await.result {
      for {
        _ <- client.get.addAuth(Auth("digest", "super:test".getBytes))
        _ <- client.get.getData("/path1")
        _ <- client.get.setACL("/path1", Ids.READ_ACL_UNSAFE, -1)
        _ <- client.get.create("/path1/foo", "".getBytes, Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT)
        _ <- client.get.setACL("/path1", Ids.OPEN_ACL_UNSAFE, -1)
        _ <- client.get.delete("/path1/foo", -1)
        _ <- client.get.delete("/path1", -1)
      } None
    }

    disconnect()
    Await.ready(client.get.closeService())
  }

  test("Auth super test 2") {
    newClient()
    connect()

    val rep = Await.result {
      for {
        _ <- client.get.addAuth(Auth("digest", "pat:pass".getBytes))
        create <- client.get.create("/path10", "h".getBytes, Ids.CREATOR_ALL_ACL,
          CreateMode.PERSISTENT)
      } yield create
    }

    assert(rep === "/path10")
    disconnect()
    Await.ready(client.get.closeService())

    newClient()
    connect()

    intercept[NoAuthException] {
      Await.result {
        client.get.getData("/path10")
      }
    }

    disconnect()
    Await.ready(client.get.closeService())

    newClient()
    connect()

    intercept[NoAuthException] {
      Await.result {
        client.get.addAuth(Auth("digest", "pat:pass2".getBytes)) before
          client.get.getData("/path10")
      }
    }

    disconnect()
    Await.ready(client.get.closeService())

    newClient()
    connect()

    intercept[NoAuthException] {
      Await.result {
        client.get.addAuth(Auth("digest", "super:test2".getBytes)) before
          client.get.getData("/path10")
      }
    }

    disconnect()
    Await.ready(client.get.closeService())

    newClient()
    connect()

    Await.result {
      client.get.addAuth(Auth("digest", "pat:pass".getBytes)) before
        client.get.getData("/path10").unit before client.get.delete("/path10", -1)
    }

    disconnect()
    Await.ready(client.get.closeService())
  }
}