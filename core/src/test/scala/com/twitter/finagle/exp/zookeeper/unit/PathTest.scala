package com.twitter.finagle.exp.zookeeper.unit

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import com.twitter.finagle.exp.zookeeper.utils.PathUtils
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PathTest extends FunSuite {
  test("validate paths") {
    PathUtils.validatePath("/")
    PathUtils.validatePath("/zookeeper")
    PathUtils.validatePath("/zk01")
    PathUtils.validatePath("/zookeeper/edd-001")
    PathUtils.validatePath("/zookeeper/test/hello")
  }

  test("not validate paths") {
    intercept[IllegalArgumentException] {
      PathUtils.validatePath("zookeeper/test/hello")
    }
    intercept[IllegalArgumentException] {
      PathUtils.validatePath("/zookeeper//hello")
    }
    intercept[IllegalArgumentException] {
      PathUtils.validatePath("/zookeeper//hello")
    }
    intercept[IllegalArgumentException] {
      PathUtils.validatePath("")
    }
    intercept[IllegalArgumentException] {
      PathUtils.validatePath("zk")
    }
  }
}
