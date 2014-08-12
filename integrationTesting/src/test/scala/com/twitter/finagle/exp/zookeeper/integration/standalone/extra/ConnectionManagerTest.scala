package com.twitter.finagle.exp.zookeeper.integration.standalone.extra

import com.twitter.finagle.exp.zookeeper.NoServerFound
import com.twitter.finagle.exp.zookeeper.connection.ConnectionManager
import com.twitter.finagle.exp.zookeeper.connection.HostUtilities.ServerNotAvailable
import com.twitter.finagle.exp.zookeeper.integration.standalone.StandaloneIntegrationConfig
import com.twitter.util.Await
import com.twitter.util.TimeConversions._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ConnectionManagerTest extends FunSuite with StandaloneIntegrationConfig {
  trait HelperTrait {
    val connectionManager = new ConnectionManager(
      ipAddress + ":" + port,
      None,
      true,
      Some(1.minute),
      Some(1.minute)
    )
  }

  test("should close the connection manager") {
    new HelperTrait {
      val ret = Await.result {
        connectionManager.initConnectionManager() before
          connectionManager.close() before
          connectionManager.hasAvailableService
      }
      assert(ret === false)
      Await.result(connectionManager.close())
    }
  }

  test("test should connect to a server") {
    new HelperTrait {
      val ret = Await.result {
        connectionManager.initConnectionManager() before
          connectionManager.close() before
          connectionManager.findAndConnect() before
          connectionManager.hasAvailableService
      }
      assert(ret === true)
      Await.result(connectionManager.close())
    }
  }

  test("should find and connect to a server") {
    new HelperTrait {
      val ret = Await.result {
        connectionManager.initConnectionManager() before
          connectionManager.findAndConnect() before
          connectionManager.hasAvailableService
      }
      assert(ret === true)
      Await.result(connectionManager.close())
    }
  }

  test("should test and connect to a server") {
    new HelperTrait {
      val ret = Await.result {
        connectionManager.initConnectionManager() before
          connectionManager.testAndConnect(ipAddress + ":" + port) before
          connectionManager.hasAvailableService
      }
      assert(ret === true)
      Await.result(connectionManager.close())
    }
  }

  test("should test and not connect to a server") {
    new HelperTrait {
      intercept[ServerNotAvailable] {
        Await.result {
          connectionManager.initConnectionManager() before
            connectionManager.testAndConnect("3.4.5.6:7777")
        }
      }
      Await.result(connectionManager.close())
    }
  }

  test("should call hasAvailableService") {
    new HelperTrait {
      val ret = Await.result {
        connectionManager.initConnectionManager() before
          connectionManager.hasAvailableService
      }
      assert(ret === true)
      Await.result(connectionManager.close())
      val ret2 = Await.result {
        connectionManager.close() before
          connectionManager.hasAvailableService
      }
      assert(ret2 === false)
      Await.result(connectionManager.close())
    }
  }

  test("should init connection manager") {
    new HelperTrait {
      val ret = Await.result {
        connectionManager.initConnectionManager() before
          connectionManager.hasAvailableService
      }
      assert(ret === true)
      Await.result(connectionManager.close())
    }
  }
}