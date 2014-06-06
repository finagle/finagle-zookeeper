package com.twitter.finagle.exp.zookeeper.utils

import com.twitter.finagle.exp.zookeeper.data.ACL

object Implicits {
  implicit def aclToArray(acl: ACL): Array[ACL] = Array[ACL](acl)
}
