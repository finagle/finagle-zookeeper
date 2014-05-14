package com.twitter.finagle.exp.zookeeper

object ZookeeperDefinitions {

  /* Defines the creation mode of a znode*/
  object createMode{
    val PERSISTENT = 0
    val EPHEMERAL = 1
    val PERSISTENT_SEQUENTIAL = 2
    val EPHEMERAL_SEQUENTIAL = 3
  }

  /* XID to identify request type */
  object opCode {
    val notification = 0
    val create = 1
    val delete = 2
    val exists = 3
    val getData = 4
    val setData = 5
    val getACL = 6
    val setACL = 7
    val getChildren = 8
    val sync = 9
    val ping = 11
    val getChildren2 = 12
    val check = 13
    val multi = 14
    val auth = 100
    val setWatches = 101
    val sasl = 102
    val createSession = -10
    val closeSession = -11
    val error = -1
  }

  /* Error code returned by the server */
  object errorCode {
    val OK = 0
    val SYSTEM_ERROR = -1
    val RUNTIME_INCONSISTENCY = -2
    val DATA_INCONSISTENCY = -3
    val CONNECTION_LOSS = -4
    val MARSHALLING_ERROR = -5
    val UNIMPLEMENTED = -6
    val OPERATION_TIMEOUT = -7
    val BAD_ARGUMENTS = -8
    val API_ERROR = -100
    val NO_NODE = -101
    val NO_AUTH = -102
    val BAD_VERSION = -103
    val NO_CHILDREN_FOR_EPHEMERALS = -108
    val NODE_EXISTS = -110
    val NOT_EMPTY = -111
    val SESSION_EXPIRED = -112
    val INVALID_CALLBACK = -113
    val INVALID_ACL = -114
    val AUTH_FAILED = -115
    val SESSION_MOVED = -118
    val NOT_READ_ONLY = -119

    /* Return error message from error code */
    def getError(errorCode: Int): String = errorCode match {
      case OK => "OK"
      case SYSTEM_ERROR => "System error"
      case RUNTIME_INCONSISTENCY => "Runtime inconsistency"
      case DATA_INCONSISTENCY => "Data inconsistency"
      case CONNECTION_LOSS => "Connection loss"
      case MARSHALLING_ERROR => "Marshalling error"
      case UNIMPLEMENTED => "Unimplemented"
      case OPERATION_TIMEOUT => "Operation timeout"
      case BAD_ARGUMENTS => "Bad arguments"
      case API_ERROR => "API errors"
      case NO_NODE => "Node does not exists"
      case NO_AUTH => "No authentication"
      case BAD_VERSION => "Bad version"
      case NO_CHILDREN_FOR_EPHEMERALS => "No children for ephemerals"
      case NODE_EXISTS => "Node already exists"
      case NOT_EMPTY => "Node not empty"
      case SESSION_EXPIRED => "Session has expired"
      case INVALID_CALLBACK => "Invalid callback"
      case INVALID_ACL => "Invalid ACL"
      case AUTH_FAILED => "Authentication has failed"
      case SESSION_MOVED => "Session has moved"
      case NOT_READ_ONLY => "This server does not support Read Only"
    }
  }

}
