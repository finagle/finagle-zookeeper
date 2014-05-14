package com.twitter.finagle.exp.zookeeper.integration

import org.scalatest.FunSuite
import com.twitter.finagle.exp.zookeeper.ACL
import com.twitter.finagle.exp.zookeeper.ZookeeperDefinitions.createMode
import com.twitter.util.{Future, Await}

class ToSolveTest extends FunSuite with IntegrationConfig {
  /* Configure your server here */
  override val ipAddress: String = "127.0.0.1"
  override val timeOut: Long = 1000
  override val port: Int = 2181

  /**
   * My Configuration
   * zookeeper v3.4.6 with a single server running locally on 127.0.0.1:2181
   *
   * First example runs well with a for comprehension
   *
   * Second example does some bad serialization without for
   * you will see error messages on the server interface while running it
   */

  test("A working example, correct output") {
    connect

    val res = for {
      _ <- client.get.create("/zookeeper/persistentNode1", "SEQ".getBytes, ACL.defaultACL, createMode.EPHEMERAL)
      _ <- client.get.create("/zookeeper/persistentNode2", "SEQ".getBytes, ACL.defaultACL, createMode.EPHEMERAL)
      _ <- client.get.create("/zookeeper/persistentNode3", "SEQ".getBytes, ACL.defaultACL, createMode.EPHEMERAL)
      _ <- client.get.create("/zookeeper/persistentNode4", "SEQ".getBytes, ACL.defaultACL, createMode.EPHEMERAL)
      _ <- client.get.create("/zookeeper/persistentNode5", "SEQ".getBytes, ACL.defaultACL, createMode.EPHEMERAL)
      _ <- client.get.create("/zookeeper/persistentNode6", "SEQ".getBytes, ACL.defaultACL, createMode.EPHEMERAL)
      _ <- client.get.create("/zookeeper/persistentNode7", "SEQ".getBytes, ACL.defaultACL, createMode.EPHEMERAL)
      _ <- client.get.create("/zookeeper/persistentNode8", "SEQ".getBytes, ACL.defaultACL, createMode.EPHEMERAL)
    } yield (None)


    val ret = Await.result(res)

    disconnect
  }

  test("A NOT working example, crazy output") {
    connect

    val res = Future.join(Seq(
      client.get.create("/zookeeper/persistentNode1", "SEQ".getBytes, ACL.defaultACL, createMode.EPHEMERAL),
      client.get.create("/zookeeper/persistentNode2", "SEQ".getBytes, ACL.defaultACL, createMode.EPHEMERAL),
      client.get.create("/zookeeper/persistentNode3", "SEQ".getBytes, ACL.defaultACL, createMode.EPHEMERAL),
      client.get.create("/zookeeper/persistentNode4", "SEQ".getBytes, ACL.defaultACL, createMode.EPHEMERAL),
      client.get.create("/zookeeper/persistentNode5", "SEQ".getBytes, ACL.defaultACL, createMode.EPHEMERAL),
      client.get.create("/zookeeper/persistentNode6", "SEQ".getBytes, ACL.defaultACL, createMode.EPHEMERAL),
      client.get.create("/zookeeper/persistentNode7", "SEQ".getBytes, ACL.defaultACL, createMode.EPHEMERAL),
      client.get.create("/zookeeper/persistentNode8", "SEQ".getBytes, ACL.defaultACL, createMode.EPHEMERAL)
    ))


    val ret = Await.result(res)

    disconnect
  }
}
