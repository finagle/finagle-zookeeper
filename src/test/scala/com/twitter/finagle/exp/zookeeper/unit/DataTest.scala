package com.twitter.finagle.exp.zookeeper.unit

import org.scalatest.FunSuite
import com.twitter.finagle.exp.zookeeper.{ZKExc, ID, ACL}

class DataTest extends FunSuite {

  test("ACL world is correct") {
    ACL.check(ACL.defaultACL(0))
  }

  test("ACL (world, me) is not correct") {
    intercept[ZKExc] {
      val acl = new ACL(31, new ID("world", "me"))
      ACL.check(acl)
    }
  }

  test("ACL (samba:anyone) is not correct") {
    intercept[ZKExc] {
      val acl = new ACL(31, new ID("samba", "anyone"))
      ACL.check(acl)
    }
  }

  test("ACL ip:192.168.10.1 is correct") {
    val acl = new ACL(31, new ID("ip", "192.168.10.1"))
    ACL.check(acl)
  }

  test("ACL ip:192.10.1 is not correct") {
    intercept[ZKExc] {
      val acl = new ACL(31, new ID("ip", "192.10.1"))
      ACL.check(acl)
    }
  }

  test("ACL ip:192.10.10.e is not correct") {
    intercept[ZKExc] {
      val acl = new ACL(31, new ID("ip", "192.10.10.e"))
      ACL.check(acl)
    }
  }

  test("ACL (auth:) is correct") {
    val acl = new ACL(31, new ID("auth", ""))
    ACL.check(acl)
  }

  test("ACL (auth:terminator) is not correct") {
    intercept[ZKExc] {
      val acl = new ACL(31, new ID("auth", "terminator"))
      ACL.check(acl)
    }
  }

  test("ACL (digest:paul:paulpwd) is correct") {
    val acl = new ACL(31, new ID("digest", "paul:paulpwd"))
    ACL.check(acl)
  }

  test("ACL (digest:paul;paulpwd) is not correct") {
    intercept[ZKExc] {
      val acl = new ACL(31, new ID("digest", "paul;paulpwd"))
      ACL.check(acl)
    }
  }
}
