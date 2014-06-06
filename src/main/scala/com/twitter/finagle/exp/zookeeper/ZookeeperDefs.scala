package com.twitter.finagle.exp.zookeeper

object ZookeeperDefs {

  val CONFIG_NODE = "/zookeeper/config"

  /* Defines the creation mode of a znode*/
  object CreateMode {
    val PERSISTENT = 0
    val EPHEMERAL = 1
    val PERSISTENT_SEQUENTIAL = 2
    val EPHEMERAL_SEQUENTIAL = 3
  }

  /* XID to identify the request type */
  object OpCode {
    val NOTIFICATION = 0
    val CREATE = 1
    val DELETE = 2
    val EXISTS = 3
    val GET_DATA = 4
    val SET_DATA = 5
    val GET_ACL = 6
    val SET_ACL = 7
    val GET_CHILDREN = 8
    val SYNC = 9
    val PING = 11
    val GET_CHILDREN2 = 12
    val CHECK = 13
    val MULTI = 14
    val CREATE2 = 15
    val RECONFIG = 16
    val CHECK_WATCHES = 17
    val REMOVE_WATCHES = 18
    val AUTH = 100
    val SET_WATCHES = 101
    val SASL = 102
    val CREATE_SESSION = -10
    val CLOSE_SESSION = -11
    val ERROR = -1
  }
}