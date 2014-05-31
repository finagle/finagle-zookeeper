package com.twitter.finagle.exp.zookeeper.unit

import org.scalatest.FunSuite
import com.twitter.finagle.exp.zookeeper.utils.PathUtils

class PathTest extends FunSuite {

  test("validate path /") {
    PathUtils.validatePath("/")
  }

  test("validate path /zookeeper/test/hello") {
    PathUtils.validatePath("/zookeeper/test/hello")
  }

  test("not validate path zookeeper/test/hello") {
    intercept[IllegalArgumentException] {
      PathUtils.validatePath("zookeeper/test/hello")
    }
  }

  test("not validate path /zookeeper//hello") {
    intercept[IllegalArgumentException] {
      PathUtils.validatePath("/zookeeper//hello")
    }
  }
}
