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

  object Ids {
    /**
     * This Id represents anyone.
     */
    val ANYONE_ID_UNSAFE = new Id("world", "anyone")

    /**
     * This Id is only usable to set ACLs. It will get substituted with the
     * Id's the client authenticated with.
     */
    val AUTH_IDS = new Id("auth", "")

    /**
     * This is a completely open ACL .
     */
    val OPEN_ACL_UNSAFE = Array[ACL](ACL(Perms.ALL, ANYONE_ID_UNSAFE))

    /**
     * This ACL gives the creators authentication id's all permissions.
     */
    val CREATOR_ALL_ACL = Array[ACL](ACL(Perms.ALL, AUTH_IDS))

    /**
     * This ACL gives the world the ability to read.
     */
    val READ_ACL_UNSAFE = Array[ACL](ACL(Perms.READ, ANYONE_ID_UNSAFE))
  }

  object Perms {
    val READ: Int = 1 << 0
    val WRITE: Int = 1 << 1
    val CREATE: Int = 1 << 2
    val DELETE: Int = 1 << 3
    val ADMIN: Int = 1 << 4
    val ALL: Int = READ | WRITE | CREATE | DELETE | ADMIN
    val CREATE_DELETE = CREATE | DELETE
    val READ_WRITE = READ | WRITE
  }
}