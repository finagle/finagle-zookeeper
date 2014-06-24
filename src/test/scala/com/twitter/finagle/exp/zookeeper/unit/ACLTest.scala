package com.twitter.finagle.exp.zookeeper.unit

import com.twitter.finagle.exp.zookeeper.data.ACL.Perms
import com.twitter.finagle.exp.zookeeper.data.{Id, ACL, Ids}
import com.twitter.finagle.exp.zookeeper.utils.Implicits._
import org.scalatest.FunSuite

class ACLTest extends FunSuite {

  test("ACL world is correct") {
    ACL.check(Ids.OPEN_ACL_UNSAFE)
  }

  test("ACL (world, me) is not correct") {
    intercept[IllegalArgumentException] {
      val acl = ACL(31, Id("world", "me"))
      ACL.check(acl)
    }
  }

  test("ACL (samba:anyone) is not correct") {
    intercept[IllegalArgumentException] {
      val acl = ACL(31, Id("samba", "anyone"))
      ACL.check(acl)
    }
  }

  test("ACL ip:192.168.10.1 is correct") {
    val acl = ACL(31, Id("ip", "192.168.10.1"))
    ACL.check(acl)
  }

  test("ACL ip:192.10.1 is not correct") {
    intercept[IllegalArgumentException] {
      val acl = ACL(31, Id("ip", "192.10.1"))
      ACL.check(acl)
    }
  }

  test("ACL ip:192.10.10.e is not correct") {
    intercept[IllegalArgumentException] {
      val acl = ACL(31, Id("ip", "192.10.10.e"))
      ACL.check(acl)
    }
  }

  test("ACL (auth:) is correct") {
    val acl = ACL(31, Id("auth", ""))
    ACL.check(acl)
  }

  test("ACL (auth:terminator) is not correct") {
    intercept[IllegalArgumentException] {
      val acl = ACL(31, Id("auth", "terminator"))
      ACL.check(acl)
    }
  }

  test("ACL (digest:paul:paulpwd) is correct") {
    val acl = ACL(31, Id("digest", "paul:paulpwd"))
    ACL.check(acl)
  }

  test("ACL (digest:paul;paulpwd) is not correct") {
    intercept[IllegalArgumentException] {
      val acl = ACL(31, new Id("digest", "paul;paulpwd"))
      ACL.check(acl)
    }
  }

  test("Permission from string rwcda works") {
    val perms = ACL.Perms.permFromString("rwcda")
    assert(perms === 31)
  }

  test("Permission from string hello should fail") {
    intercept[IllegalArgumentException] {
      ACL.Perms.permFromString("hello")
    }
  }

  test("Parse ACL should work") {
    val aclList = ACL.parseACL("world:anyone:rw")
    assert(aclList(0).perms === Perms.READ_WRITE)
    assert(aclList(0).id.scheme === "world")
    assert(aclList(0).id.data === "anyone")

    ACL.check(aclList)

    val aclList2 = ACL.parseACL("ip:192.168.1.0:rwcda, world:anyone:cd," +
      "auth::rw,digest:toto:pwd:cd, ip:10.0.0.1:cd")
    assert(aclList2(0).perms === Perms.ALL)
    assert(aclList2(0).id.scheme === "ip")
    assert(aclList2(0).id.data === "192.168.1.0")
    assert(aclList2(1).perms === Perms.CREATE_DELETE)
    assert(aclList2(1).id.scheme === "world")
    assert(aclList2(1).id.data === "anyone")
    assert(aclList2(2).perms === Perms.READ_WRITE)
    assert(aclList2(2).id.scheme === "auth")
    assert(aclList2(2).id.data === "")
    assert(aclList2(3).perms === Perms.CREATE_DELETE)
    assert(aclList2(3).id.scheme === "digest")
    assert(aclList2(3).id.data === "toto:pwd")
    assert(aclList2(4).perms === Perms.CREATE_DELETE)
    assert(aclList2(4).id.scheme === "ip")
    assert(aclList2(4).id.data === "10.0.0.1")

    ACL.check(aclList2)
  }

  test("Parse ACL should not work") {
    intercept[IllegalArgumentException] {
      ACL.parseACL("worldanyone:rw")
    }
    intercept[IllegalArgumentException] {
      ACL.parseACL("world:anyone:how")
    }
    intercept[IllegalArgumentException] {
      ACL.parseACL("world:anyone:31")
    }
    intercept[IllegalArgumentException] {
      ACL.parseACL("world:anyone:rw, ip:10.0.1.10:auth:")
    }
    intercept[IllegalArgumentException] {
      ACL.parseACL("world:anyone:rwip:10.0.0.1")
    }
    intercept[IllegalArgumentException] {
      ACL.parseACL("worldanyone")
    }
    intercept[IllegalArgumentException] {
      ACL.parseACL("world:anyone")
    }
    intercept[IllegalArgumentException] {
      ACL.parseACL("auth:rw")
    }
    intercept[IllegalArgumentException] {
      ACL.parseACL(":rw:")
    }
    intercept[IllegalArgumentException] {
      ACL.parseACL("rw:")
    }
  }

}
