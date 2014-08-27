package com.twitter.finagle.exp.zookeeper.integration.standalone.v3_4.command

import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.CreateMode
import com.twitter.finagle.exp.zookeeper.data.ACL.Perms
import com.twitter.finagle.exp.zookeeper.data.{ACL, Id, Ids}
import com.twitter.finagle.exp.zookeeper.integration.standalone.StandaloneIntegrationConfig
import com.twitter.finagle.exp.zookeeper.{InvalidAclException, NoAuthException}
import com.twitter.util.Await
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AclTest extends FunSuite with StandaloneIntegrationConfig {
  test("Basic add auth") {
    newClient()
    connect()

    client.get.addAuth("digest", "pat:test".getBytes)
    client.get.setACL("/", Ids.CREATOR_ALL_ACL, -1)

    disconnect()
    Await.ready(client.get.close())
  }

  test("acl count") {
    newClient()
    connect()

    val acls = Seq[ACL](
      ACL(Perms.READ, Ids.ANYONE_ID_UNSAFE),
      ACL(Perms.ALL, Ids.AUTH_IDS),
      ACL(Perms.READ, Ids.ANYONE_ID_UNSAFE),
      ACL(Perms.ALL, Ids.AUTH_IDS)
    )

    val rep = for {
      _ <- client.get.addAuth("digest", "pat:test".getBytes)
      _ <- client.get.setACL("/", Ids.CREATOR_ALL_ACL, -1)
      _ <- client.get.create("/path", "hello".getBytes, acls, CreateMode.EPHEMERAL)
      acl <- client.get.getACL("/path")
    } yield acl

    assert(Await.result(rep).acl.size === 2)

    disconnect()
    Await.ready(client.get.close())
  }

  test("root acl is correct") {
    newClient()
    connect()

    Await.result(
      for {
        _ <- client.get.addAuth("digest", "pat:test".getBytes)
        _ <- client.get.setACL("/", Ids.CREATOR_ALL_ACL, -1)
        _ <- client.get.getData("/")
      } yield None
    )

    disconnect()
    Await.ready(client.get.close())

    newClient()
    connect()

    intercept[NoAuthException] {
      Await.result(client.get.getData("/"))
    }
    intercept[InvalidAclException] {
      Await.result(client.get.create("/apps", "hello".getBytes, Ids.CREATOR_ALL_ACL,
        CreateMode.PERSISTENT))
    }

    Await.result(client.get.addAuth("digest", "world:anyone".getBytes))

    intercept[NoAuthException] {
      Await.result(client.get.create("/apps", "hello".getBytes, Ids.CREATOR_ALL_ACL,
        CreateMode.PERSISTENT))
    }

    disconnect()
    connect()

    Await.result(
      for {
        _ <- client.get.addAuth("digest", "pat:test".getBytes)
        _ <- client.get.getData("/")
        _ <- client.get.create(
          "/apps",
          "hello".getBytes,
          Ids.CREATOR_ALL_ACL,
          CreateMode.PERSISTENT
        )
        _ <- client.get.delete("/apps", -1)
        _ <- client.get.setACL("/", Ids.OPEN_ACL_UNSAFE, -1)
      } yield None
    )

    disconnect()
    connect()

    Await.result(
      client.get.getData("/").unit before
        client.get.create(
          "/apps",
          "hello".getBytes,
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT
        )
    )

    intercept[InvalidAclException] {
      Await.result(client.get.create(
        "/apps",
        "hello".getBytes,
        Ids.CREATOR_ALL_ACL,
        CreateMode.PERSISTENT))
    }

    Await.ready(
      for {
        _ <- client.get.delete("/apps", -1)
        _ <- client.get.addAuth("digest", "world:anyone".getBytes)
        _ <- client.get.create(
          "/apps", "hello".getBytes, Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT)
        _ <- client.get.disconnect()
        _ <- client.get.connect()
        _ <- client.get.delete("/apps", -1)
      } yield None
    )

    disconnect()
    Await.ready(client.get.close())
  }

  test("global acl test") {
    newClient()
    connect()

    intercept[InvalidAclException] {
      Await.result {
        client.get.create(
          "/acltest",
          "".getBytes,
          Ids.CREATOR_ALL_ACL,
          CreateMode.PERSISTENT)
      }
    }

    val acls = Seq[ACL](
      ACL(Perms.ALL | Perms.ADMIN, Ids.AUTH_IDS),
      ACL(Perms.ALL | Perms.ADMIN, Id("ip", "127.0.0.1/8"))
    )

    intercept[IllegalArgumentException] {
      Await.result {
        client.get.create("/acltest", "".getBytes, acls, CreateMode.PERSISTENT)
      }
    }

    Await.result {
      client.get.addAuth("digest", "ben:passwd".getBytes) before
        client.get.create("/acltest", "".getBytes, Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT)
    }

    disconnect()
    Await.ready(client.get.close())

    newClient()
    connect()

    Await.result {
      client.get.addAuth("digest", "ben:passwd2".getBytes)
    }

    intercept[NoAuthException] {
      Await.result {
        client.get.getData("/acltest")
      }
    }

    Await.result {
      client.get.addAuth("digest", "ben:passwd".getBytes) before
        client.get.getData("/acltest").unit before
        client.get.setACL("/acltest", Ids.OPEN_ACL_UNSAFE, -1)
    }

    disconnect()
    Await.ready(client.get.close())

    newClient()
    connect()

    val rep = Await.result {
      for {
        _ <- client.get.getData("/acltest")
        aclret <- client.get.getACL("/acltest")
      } yield aclret
    }

    assert(rep.acl.size === 1)
    assert(rep.acl === Ids.OPEN_ACL_UNSAFE)

    Await.ready {
      client.get.delete("/acltest", -1)
    }

    disconnect()
    Await.ready(client.get.close())
  }
}