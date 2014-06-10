package com.twitter.finagle.exp.zookeeper.integration

import org.scalatest.FunSuite
import com.twitter.finagle.exp.zookeeper.{CreateResult, CreateOp, OpRequest}
import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.CreateMode
import com.twitter.util.Await
import com.twitter.finagle.exp.zookeeper.data.{Ids, ACL}

class TransactionTest extends FunSuite with IntegrationConfig {
  /* Configure your server here */
  val ipAddress: String = "127.0.0.1"
  val port: Int = 2181
  val timeOut: Long = 1000

  test("Server is up") {
    assert(isPortAvailable === false)
  }

  test("Small create Transaction works") {
    newClient()
    connect()

    val opList = Array[OpRequest](new CreateOp("/zookeeper/hello", "TRANS".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL),
      new CreateOp("/zookeeper/world", "TRANS".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL))

    val res = client.get.transaction(opList)
    val fut = Await.result(res)

    assert(fut.responseList(0).asInstanceOf[CreateResult].path === "/zookeeper/hello")
    assert(fut.responseList(1).asInstanceOf[CreateResult].path === "/zookeeper/world")

    disconnect()
    Await.ready(client.get.closeService)
  }

}
