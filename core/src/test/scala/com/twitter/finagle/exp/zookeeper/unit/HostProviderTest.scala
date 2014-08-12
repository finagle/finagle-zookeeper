package com.twitter.finagle.exp.zookeeper.unit

import com.twitter.finagle.exp.zookeeper.connection.{HostUtilities, HostProvider}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HostProviderTest extends FunSuite {
  test("should add some hosts") {
    val hostProvider = new HostProvider("127.0.0.1:2181", true, None, None)
    hostProvider.addHost("10.0.0.1:2181,10.0.0.10:2181,192.168.0.1:2181")
    assert(hostProvider.serverList.contains("10.0.0.1:2181"))
    assert(hostProvider.serverList.contains("10.0.0.10:2181"))
    assert(hostProvider.serverList.contains("192.168.0.1:2181"))
    intercept[IllegalArgumentException] {
      hostProvider.addHost("this is not legal")
    }
    intercept[IllegalArgumentException] {
      hostProvider.addHost("10.0.1:2181")
    }
    intercept[IllegalArgumentException] {
      hostProvider.addHost("10.0.0.1:2181;127.0.0.1:2181")
    }
    intercept[IllegalArgumentException] {
      hostProvider.addHost("127.0.0.j1:2181")
    }
    intercept[IllegalArgumentException] {
      hostProvider.addHost("127.0.0.1")
    }
  }

  test("should shuffle the host list") {
    val seq = Seq("10.0.0.1:2181", "10.0.0.10:2181", "192.168.0.1:2181",
      "192.168.0.10:2181", "192.168.0.4:2181", "192.168.0.3:2181")
    val seq2 = HostUtilities.shuffleSeq(seq)
    assert(seq != seq2)
  }
}