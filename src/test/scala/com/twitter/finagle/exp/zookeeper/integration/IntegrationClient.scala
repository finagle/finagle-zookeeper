package com.twitter.finagle.exp.zookeeper.integration

import org.scalatest.FunSuite
import com.twitter.finagle.exp.zookeeper.client.{Client, ClientBuilder}
import com.twitter.util.{Future, Await}
import com.twitter.finagle.exp.zookeeper.ZookeeperDefinitions.createMode
import com.twitter.finagle.exp.zookeeper.ACL

class ExampleSuite extends FunSuite {

  test("Application test") {
    val logger = Client.getLogger

    val client = ClientBuilder.newClient("127.0.0.1:2181", 1000)

    val connect = client.connect
    connect onSuccess {
      a =>
        logger.info("Connected to zookeeper server: " + client.adress)
    } onFailure {
      e =>
        logger.severe("Connect Error")
    }
    Thread.sleep(1000)

    val res = for {
    // acl <- client.getACL("/zookeeper")
      _ <- client.create("/zookeeper/test", "HELLO".getBytes, ACL.defaultACL, createMode.EPHEMERAL)
      _ <- client.exists("/zookeeper/test", true)
      _ <- client.getChildren("/zookeeper/test", false)
      _ <- client.getChildren2("/", false)
      _ <- client.setData("/zookeeper/test", "CHANGE".getBytes, -1)
      ret <- client.getData("/zookeeper/test", false)
      _ <- client.setACL("/zookeeper/test", ACL.defaultACL, -1)
      _ <- client.sync("/zookeeper")
     // _ <- client.delete("/zookeeper/test", -1)
    } yield ret

    Await.result(connect)
    Thread.sleep(5000)
    client.disconnect
  }
}