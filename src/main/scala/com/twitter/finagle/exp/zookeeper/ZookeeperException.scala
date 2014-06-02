package com.twitter.finagle.exp.zookeeper

import com.twitter.finagle.exp.zookeeper.ZookeeperDefinitions.errorCode

/* Custom Zookeeper exception */
case class ZookeeperException(msg: String) extends RuntimeException(msg)
/** System and server-side errors.
  * This is never thrown by the server, it shouldn't be used other than
  * to indicate a range. Specifically error codes greater than this
  * value, but lesser than ApiErrorException, are system errors.
  */
case class SystemErrorException(override val msg: String) extends ZookeeperException(msg)
/** A runtime inconsistency was found */
case class RuntimeInconsistencyException(override val msg: String) extends ZookeeperException(msg)
/** A data inconsistency was found */
case class DataInconsistencyException(override val msg: String) extends ZookeeperException(msg)
/** Connection to the server has been lost */
case class ConnectionLossException(override val msg: String) extends ZookeeperException(msg)
/** Error while marshalling or unmarshalling data */
case class MarshallingErrorException(override val msg: String) extends ZookeeperException(msg)
/** Operation is unimplemented */
case class UnimplementedException(override val msg: String) extends ZookeeperException(msg)
/** Operation timeout */
case class OperationTimeoutException(override val msg: String) extends ZookeeperException(msg)
/** Invalid arguments */
case class BadArgumentsException(override val msg: String) extends ZookeeperException(msg)
/** No quorum of new config is connected and up-to-date with the leader of last commmitted config - try
  * invoking reconfiguration after new servers are connected and synced */
case class NewConfigNoQuorumException(override val msg: String) extends ZookeeperException(msg)
/** Another reconfiguration is in progress -- concurrent reconfigs not supported (yet) */
case class ReconfigInProgressException(override val msg: String) extends ZookeeperException(msg)
/** Unknown session (internal server use only) */
case class UnknownSessionException(override val msg: String) extends ZookeeperException(msg)

/** API errors.
  * This is never thrown by the server, it shouldn't be used other than
  * to indicate a range. Specifically error codes greater than this
  * value are API errors (while values less than this indicate a
  * SystemErrorException
  */
case class ApiErrorException(override val msg: String) extends ZookeeperException(msg)
/** Node does not exist */
case class NoNodeException(override val msg: String) extends ZookeeperException(msg)
/** Not authenticated */
case class NoAuthException(override val msg: String) extends ZookeeperException(msg)
/** Version conflict */
case class BadVersionException(override val msg: String) extends ZookeeperException(msg)
/** Ephemeral nodes may not have children */
case class NoChildrenForEphemeralsException(override val msg: String) extends ZookeeperException(msg)
/** The node already exists */
case class NodeExistsException(override val msg: String) extends ZookeeperException(msg)
/** The node has children */
case class NotEmptyException(override val msg: String) extends ZookeeperException(msg)
/** The session has been expired by the server */
case class SessionExpiredException(override val msg: String) extends ZookeeperException(msg)
/** Invalid callback specified */
case class InvalidCallbackException(override val msg: String) extends ZookeeperException(msg)
/** Invalid ACL specified */
case class InvalidAclException(override val msg: String) extends ZookeeperException(msg)
/** Client authentication failed */
case class AuthFailedException(override val msg: String) extends ZookeeperException(msg)
/** Session moved to another server, so operation is ignored */
case class SessionMovedException(override val msg: String) extends ZookeeperException(msg)
/** State-changing request is passed to read-only server */
case class NotReadOnlyException(override val msg: String) extends ZookeeperException(msg)
/** Attempt to create ephemeral node on a local session */
case class EphemeralOnLocalSessionException(override val msg: String) extends ZookeeperException(msg)
/** Attempts to remove a non-existing watcher */
case class NoWatcherException(override val msg: String) extends ZookeeperException(msg)

/** ZooKeeper client internal error */
case class ZkDispatchingException(override val msg: String) extends ZookeeperException(msg)

object ZookeeperException {
  def create(msg: String): ZookeeperException = new ZookeeperException(msg)
  def create(msg: String, cause: Throwable): Throwable = new ZookeeperException(msg).initCause(cause)
  def create(code: Int): ZookeeperException = create("", code)

  def create(msg: String, code: Int): ZookeeperException = code match {
    // case 0 => Ok opCode
    case -1 => new SystemErrorException(msg + " " + errorCode.getError(code))
    case -2 => new RuntimeInconsistencyException(msg + " " + errorCode.getError(code))
    case -3 => new DataInconsistencyException(msg + " " + errorCode.getError(code))
    case -4 => new ConnectionLossException(msg + " " + errorCode.getError(code))
    case -5 => new MarshallingErrorException(msg + " " + errorCode.getError(code))
    case -6 => new UnimplementedException(msg + " " + errorCode.getError(code))
    case -7 => new OperationTimeoutException(msg + " " + errorCode.getError(code))
    case -8 => new BadArgumentsException(msg + " " + errorCode.getError(code))
    case -12 => new UnknownSessionException(msg + " " + errorCode.getError(code))
    case -13 => new NewConfigNoQuorumException(msg + " " + errorCode.getError(code))
    case -14 => new ReconfigInProgressException(msg + " " + errorCode.getError(code))
    case -100 => new ApiErrorException(msg + " " + errorCode.getError(code))
    case -101 => new NoNodeException(msg + " " + errorCode.getError(code))
    case -102 => new NoAuthException(msg + " " + errorCode.getError(code))
    case -103 => new BadVersionException(msg + " " + errorCode.getError(code))
    case -108 => new NoChildrenForEphemeralsException(msg + " " + errorCode.getError(code))
    case -110 => new NodeExistsException(msg + " " + errorCode.getError(code))
    case -111 => new NotEmptyException(msg + " " + errorCode.getError(code))
    case -112 => new SessionExpiredException(msg + " " + errorCode.getError(code))
    case -113 => new InvalidCallbackException(msg + " " + errorCode.getError(code))
    case -114 => new InvalidAclException(msg + " " + errorCode.getError(code))
    case -115 => new AuthFailedException(msg + " " + errorCode.getError(code))
    case -118 => new SessionMovedException(msg + " " + errorCode.getError(code))
    case -119 => new SessionExpiredException(msg + " " + errorCode.getError(code))
    case -120 => new EphemeralOnLocalSessionException(msg + " " + errorCode.getError(code))
    case -121 => new NoWatcherException(msg + " " + errorCode.getError(code))
  }
}