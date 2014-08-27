package com.twitter.finagle.exp.zookeeper.integration.quorum.v3_5.command

import java.util

import com.twitter.finagle.exp.zookeeper.integration.quorum.QuorumIntegrationConfig
import com.twitter.util.Await
import org.scalatest.FunSuite

class GetConfigTest extends FunSuite with QuorumIntegrationConfig {
  test("Basic get config") {
    newClients()
    connectClients()

    val rep = Await.result(client1.get.getConfig())
    assert(rep.data.size > 0)

    disconnectClients()
    closeServices()
  }

  test("All clients have the same configuration") {
    newClients()
    connectClients()

    val rep = Await.result(client1.get.getConfig())
    val rep2 = Await.result(client2.get.getConfig())
    val rep3 = Await.result(client3.get.getConfig())

    assert(util.Arrays.equals(rep.data, rep2.data))
    assert(util.Arrays.equals(rep3.data, rep2.data))

    disconnectClients()
    closeServices()
  }
}