package com.twitter.finagle.exp.zookeeper.integration.standalone.command

import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.CreateMode
import com.twitter.finagle.exp.zookeeper._
import com.twitter.finagle.exp.zookeeper.data.Ids
import com.twitter.finagle.exp.zookeeper.integration.standalone.StandaloneIntegrationConfig
import com.twitter.util.Await
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TransactionTest extends FunSuite with StandaloneIntegrationConfig {
  test("Small create Transaction works") {
    newClient()
    connect()

    val opList = Seq(
      CreateRequest(
        "/zookeeper/hello", "TRANS".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL),
      CreateRequest(
        "/zookeeper/world", "TRANS".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL))

    val res = client.get.transaction(opList)
    val finalRep = Await.result(res)

    assert(finalRep.responseList(0).asInstanceOf[CreateResponse].path === "/zookeeper/hello")
    assert(finalRep.responseList(1).asInstanceOf[CreateResponse].path === "/zookeeper/world")

    disconnect()
    Await.ready(client.get.close())
  }

  test("Create and delete") {
    newClient()
    connect()

    val opList = Seq(
      CreateRequest(
        "/zookeeper/hello", "TRANS".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL),
      DeleteRequest("/zookeeper/hello", -1))

    val res = client.get.transaction(opList)
    val finalRep = Await.result(res)

    assert(finalRep.responseList(0).asInstanceOf[CreateResponse].path === "/zookeeper/hello")

    disconnect()
    Await.ready(client.get.close())
  }

  test("Create , set and delete with error") {
    newClient()
    connect()

    val opList = Seq(
      CreateRequest(
        "/zookeeper/hello", "TRANS".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL),
      SetDataRequest("/zookeeper/hello", "changing".getBytes, -1),
      DeleteRequest("/zookeeper/hell", -1)
    )

    val res = client.get.transaction(opList)
    val finalRep = Await.result(res)

    assert(finalRep.responseList(0).asInstanceOf[ErrorResponse].exception.isInstanceOf[OkException])
    assert(finalRep.responseList(1).asInstanceOf[ErrorResponse].exception.isInstanceOf[OkException])
    assert(finalRep.responseList(2).asInstanceOf[ErrorResponse].exception.isInstanceOf[NoNodeException])

    disconnect()
    Await.ready(client.get.close())
  }

  test("Create, checkVersion") {
    newClient()
    connect()

    val opList = Seq(
      CreateRequest(
        "/zookeeper/hello", "TRANS".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL),
      CheckVersionRequest("/zookeeper/hello", 0)
    )

    val res = client.get.transaction(opList)
    val finalRep = Await.result(res)

    assert(finalRep.responseList(0).isInstanceOf[CreateResponse])
    assert(finalRep.responseList(1).isInstanceOf[EmptyResponse])

    disconnect()
    Await.ready(client.get.close())
  }

  test("Create, set and checkVersion") {
    newClient()
    connect()

    val opList = Seq(
      CreateRequest(
        "/zookeeper/hello", "TRANS".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL),
      SetDataRequest("/zookeeper/hello", "changing".getBytes, -1),
      CheckVersionRequest("/zookeeper/hello", 1)
    )

    val res = client.get.transaction(opList)
    val finalRep = Await.result(res)

    assert(finalRep.responseList(0).isInstanceOf[CreateResponse])
    assert(finalRep.responseList(1).isInstanceOf[SetDataResponse])
    assert(finalRep.responseList(2).isInstanceOf[EmptyResponse])

    disconnect()
    Await.ready(client.get.close())
  }

  test("Create, set(x12), checkVersion and delete") {
    newClient()
    connect()

    val opList = Seq(
      CreateRequest(
        "/zookeeper/hello", "TRANS".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL),
      SetDataRequest("/zookeeper/hello", "changing".getBytes, -1),
      SetDataRequest("/zookeeper/hello", "chang257ing".getBytes, -1),
      SetDataRequest("/zookeeper/hello", "chang5ig".getBytes, -1),
      SetDataRequest("/zookeeper/hello", "chan1257ing".getBytes, -1),
      SetDataRequest("/zookeeper/hello", "chai4n42g".getBytes, -1),
      SetDataRequest("/zookeeper/hello", "cha7ngng".getBytes, -1),
      SetDataRequest("/zookeeper/hello", "chan542ing".getBytes, -1),
      SetDataRequest("/zookeeper/hello", "cha8i4ng".getBytes, -1),
      SetDataRequest("/zookeeper/hello", "chi752ng".getBytes, -1),
      SetDataRequest("/zookeeper/hello", "cha64nng".getBytes, -1),
      SetDataRequest("/zookeeper/hello", "ch7ann5g".getBytes, -1),
      CheckVersionRequest("/zookeeper/hello", 11),
      DeleteRequest("/zookeeper/hello", 11))

    val res = client.get.transaction(opList)
    val finalRep = Await.result(res)

    assert(finalRep.responseList(0).isInstanceOf[CreateResponse])
    assert(finalRep.responseList(5).isInstanceOf[SetDataResponse])
    assert(finalRep.responseList(12).isInstanceOf[EmptyResponse])
    assert(finalRep.responseList(13).isInstanceOf[EmptyResponse])

    disconnect()
    Await.ready(client.get.close())
  }
}
