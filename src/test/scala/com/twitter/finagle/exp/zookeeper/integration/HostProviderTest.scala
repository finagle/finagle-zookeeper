package com.twitter.finagle.exp.zookeeper.integration

import com.twitter.finagle.exp.zookeeper.connection.HostProvider
import com.twitter.util.Await
import com.twitter.util.TimeConversions._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HostProviderTest extends FunSuite with IntegrationConfig {
  trait HelperTrait {
    val hostProvider = new HostProvider(
      ipAddress + ":" + port,
      true,
      Some(1.minute),
      Some(1.minute)
    )
  }

  test("should test a host") {
    new HelperTrait {
      val ret = Await.result {
        hostProvider.testHost(ipAddress + ":" + port)
      }
      assert(ret === true)
    }
  }

  test("should test a host or find a new one") {
    new HelperTrait {
      val ret = Await.result {
        hostProvider.testOrFind(ipAddress + ":" + port)
      }
      assert(ret === ipAddress + ":" + port)
      val ret2 = Await.result {
        hostProvider.testOrFind("1.2.3.4:9999")
      }
      assert(ret2 === ipAddress + ":" + port)
    }
  }

  test("should find a host from a server list or no") {
    new HelperTrait {
      val ret = Await.result {
        hostProvider.findServer(Some(Seq(ipAddress + ":" + port)))
      }
      assert(ret === ipAddress + ":" + port)

      val ret2 = Await.result {
        hostProvider.findServer(None)
      }
      assert(ret2 === ipAddress + ":" + port)
    }
  }

  test("test rw mode server search") {
    new HelperTrait {
      val ret = Await.result {
        hostProvider.startRwServerSearch()
      }
      assert(ret === ipAddress + ":" + port)
    }
  }

  test("should test a server with isro request") {
    new HelperTrait {
      val ret = Await.result {
        hostProvider.withIsroRequest(ipAddress+":"+port)
      }
      assert(ret === ipAddress + ":" + port)
    }
  }

  test("should test a server with connect request") {
    new HelperTrait {
      val ret = Await.result {
        hostProvider.withConnectRequest(ipAddress+":"+port)
      }
      assert(ret === ipAddress + ":" + port)
    }
  }
}