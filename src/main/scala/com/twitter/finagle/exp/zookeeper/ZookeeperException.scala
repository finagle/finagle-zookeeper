package com.twitter.finagle.exp.zookeeper

/* Custom Zookeeper exception */
class ZookeeperException(msg: String) extends RuntimeException(msg)
/** System and server-side errors.
  * This is never thrown by the server, it shouldn't be used other than
  * to indicate a range. Specifically error codes greater than this
  * value, but lesser than ApiErrorException, are system errors.
  */
case class SystemErrorException(msg: String) extends ZookeeperException(msg)
/** A runtime inconsistency was found */
case class RuntimeInconsistencyException(msg: String) extends ZookeeperException(msg)
/** A data inconsistency was found */
case class DataInconsistencyException(msg: String) extends ZookeeperException(msg)
/** Connection to the server has been lost */
case class ConnectionLossException(msg: String) extends ZookeeperException(msg)
/** Error while marshalling or unmarshalling data */
case class MarshallingErrorException(msg: String) extends ZookeeperException(msg)
/** Operation is unimplemented */
case class UnimplementedException(msg: String) extends ZookeeperException(msg)
/** Operation timeout */
case class OperationTimeoutException(msg: String) extends ZookeeperException(msg)
/** Invalid arguments */
case class BadArgumentsException(msg: String) extends ZookeeperException(msg)
/** No quorum of new config is connected and up-to-date with the leader of last committed config - try
  * invoking reconfiguration after new servers are connected and synced */
case class NewConfigNoQuorumException(msg: String) extends ZookeeperException(msg)
/** Another reconfiguration is in progress -- concurrent reconfigs not supported (yet) */
case class ReconfigInProgressException(msg: String) extends ZookeeperException(msg)

/** API errors.
  * This is never thrown by the server, it shouldn't be used other than
  * to indicate a range. Specifically error codes greater than this
  * value are API errors (while values less than this indicate a
  * SystemErrorException
  */
case class ApiErrorException(msg: String) extends ZookeeperException(msg)
/** Node does not exist */
case class NoNodeException(msg: String) extends ZookeeperException(msg)
/** Not authenticated */
case class NoAuthException(msg: String) extends ZookeeperException(msg)
/** Version conflict */
case class BadVersionException(msg: String) extends ZookeeperException(msg)
/** Ephemeral nodes may not have children */
case class NoChildrenForEphemeralsException(msg: String) extends ZookeeperException(msg)
/** The node already exists */
case class NodeExistsException(msg: String) extends ZookeeperException(msg)
/** The node has children */
case class NotEmptyException(msg: String) extends ZookeeperException(msg)
/** The session has been expired by the server */
case class SessionExpiredException(msg: String) extends ZookeeperException(msg)
/** Invalid callback specified */
case class InvalidCallbackException(msg: String) extends ZookeeperException(msg)
/** Invalid ACL specified */
case class InvalidAclException(msg: String) extends ZookeeperException(msg)
/** Client authentication failed */
case class AuthFailedException(msg: String) extends ZookeeperException(msg)
/** Session moved to another server, so operation is ignored */
case class SessionMovedException(msg: String) extends ZookeeperException(msg)
/** State-changing request is passed to read-only server */
case class NotReadOnlyException(msg: String) extends ZookeeperException(msg)
/** Attempt to create ephemeral node on a local session */
case class EphemeralOnLocalSessionException(msg: String) extends ZookeeperException(msg)
/** Attempts to remove a non-existing watcher */
case class NoWatcherException(msg: String) extends ZookeeperException(msg)

/** ZooKeeper client internal error */
case class ZkDispatchingException(msg: String) extends ZookeeperException(msg)
/** Error while decoding a Buf */
case class ZkDecodingException(msg: String) extends ZookeeperException(msg)

object ZookeeperException {
  def create(msg: String): ZookeeperException = new ZookeeperException(msg)
  def create(msg: String, cause: Throwable): Throwable = new ZookeeperException(msg).initCause(cause)
  def create(code: Int): ZookeeperException = create("", code)

  def create(msg: String, code: Int): ZookeeperException = code match {
    // case 0 => Ok opCode
    case -1 => new SystemErrorException(msg + " " + "System error")
    case -2 => new RuntimeInconsistencyException(msg + " " + "Runtime inconsistency")
    case -3 => new DataInconsistencyException(msg + " " + "Data inconsistency")
    case -4 => new ConnectionLossException(msg + " " + "Connection loss")
    case -5 => new MarshallingErrorException(msg + " " + "Marshalling error")
    case -6 => new UnimplementedException(msg + " " + "Unimplemented")
    case -7 => new OperationTimeoutException(msg + " " + "Operation timeout")
    case -8 => new BadArgumentsException(msg + " " + "Bad arguments")
    case -13 => new NewConfigNoQuorumException(msg + " " +
      "No quorum of new config is connected and up-to-date with the leader of last committed config")
    case -14 => new ReconfigInProgressException(msg + " " + "Another reconfiguration is in progress")
    case -100 => new ApiErrorException(msg + " " + "API errors")
    case -101 => new NoNodeException(msg + " " + "Node does not exists")
    case -102 => new NoAuthException(msg + " " + "No authentication")
    case -103 => new BadVersionException(msg + " " + "Bad version")
    case -108 => new NoChildrenForEphemeralsException(msg + " " + "No children for ephemerals")
    case -110 => new NodeExistsException(msg + " " + "Node already exists")
    case -111 => new NotEmptyException(msg + " " + "Node not empty")
    case -112 => new SessionExpiredException(msg + " " + "Session has expired")
    case -113 => new InvalidCallbackException(msg + " " + "Invalid callback")
    case -114 => new InvalidAclException(msg + " " + "Invalid ACL")
    case -115 => new AuthFailedException(msg + " " + "Authentication has failed")
    case -118 => new SessionMovedException(msg + " " + "Session has moved")
    case -119 => new NotReadOnlyException(msg + " " + "This server does not support Read Only")
    case -120 => new EphemeralOnLocalSessionException(msg + " " +
      "Attempt to create ephemeral node on a local session")
    case -121 => new NoWatcherException(msg + " " + "Attempts to remove a non-existing watcher")
  }
}