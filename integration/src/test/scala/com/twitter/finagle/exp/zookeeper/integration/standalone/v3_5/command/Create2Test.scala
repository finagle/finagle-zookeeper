package com.twitter.finagle.exp.zookeeper.integration.standalone.v3_5.command

import com.twitter.finagle.exp.zookeeper.NodeExistsException
import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.CreateMode
import com.twitter.finagle.exp.zookeeper.data.Ids
import com.twitter.finagle.exp.zookeeper.integration.standalone.StandaloneIntegrationConfig
import com.twitter.util.Await
import org.scalatest.FunSuite

class Create2Test extends FunSuite with StandaloneIntegrationConfig {
  test("Create2 is equivalent to create + getData") {
    newClient()
    connect()

    val rep1 = Await.result {
      client.get.create2("/hello",
        "".getBytes,
        Ids.OPEN_ACL_UNSAFE,
        CreateMode.EPHEMERAL
      )
    }

    val rep2 = Await.result(client.get.getData("/hello"))

    assert(rep1.stat === rep2.stat)

    disconnect()
    Await.ready(client.get.close())
  }

  test("Create2 on existing node") {
    newClient()
    connect()

    intercept[NodeExistsException] {
      Await.result {
        client.get.create2("/hello",
          "".getBytes,
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.EPHEMERAL
        ).unit before
          client.get.create2("/hello",
            "".getBytes,
            Ids.OPEN_ACL_UNSAFE,
            CreateMode.EPHEMERAL
          )
      }
    }

    disconnect()
    Await.ready(client.get.close())
  }
}