package com.twitter.finagle.exp.zookeeper.utils

import com.twitter.finagle.exp.zookeeper.data.ACL

object Implicits {
  implicit def aclToSeq(acl: ACL): Seq[ACL] = Seq[ACL](acl)
}
