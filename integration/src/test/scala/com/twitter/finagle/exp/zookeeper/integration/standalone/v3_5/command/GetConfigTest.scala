package com.twitter.finagle.exp.zookeeper.integration.standalone.v3_5.command

import com.twitter.finagle.exp.zookeeper.ZookeeperDefs
import com.twitter.finagle.exp.zookeeper.integration.standalone.StandaloneIntegrationConfig
import com.twitter.util.Await
import org.scalatest.FunSuite

class GetConfigTest extends FunSuite with StandaloneIntegrationConfig {
  test("getConfig on standalone server is useless") {
    // We can't reconfigure a standalone server because we can't change
    // the mode during the session, only ensembles are reconfigurable
    newClient()
    connect()

    val rep = Await.result {
      client.get.sync(ZookeeperDefs.CONFIG_NODE).unit before
        client.get.getConfig()
    }

    assert(rep.data.length === 0)

    disconnect()
    Await.ready(client.get.close())
  }
}
