package com.twitter.finagle.exp.zookeeper

import com.twitter.finagle.exp.zookeeper.ZookeeperDefinitions.errorCode

/* Custom Zookeeper exception */
case class ZookeeperException(msg: String) extends RuntimeException(msg)
case class SystemErrorException(override val msg: String) extends ZookeeperException(msg)
case class RuntimeInconsistencyException(override val msg: String) extends ZookeeperException(msg)
case class DataInconsistencyException(override val msg: String) extends ZookeeperException(msg)
case class ConnectionLossException(override val msg: String) extends ZookeeperException(msg)
case class MarshallingErrorException(override val msg: String) extends ZookeeperException(msg)
case class UnimplementedException(override val msg: String) extends ZookeeperException(msg)
case class OperationTimeoutException(override val msg: String) extends ZookeeperException(msg)
case class BadArgumentsException(override val msg: String) extends ZookeeperException(msg)
case class ApiErrorException(override val msg: String) extends ZookeeperException(msg)
case class NoNodeException(override val msg: String) extends ZookeeperException(msg)
case class NoAuthException(override val msg: String) extends ZookeeperException(msg)
case class BadVersionException(override val msg: String) extends ZookeeperException(msg)
case class NoChildrenForEphemeralsException(override val msg: String) extends ZookeeperException(msg)
case class NodeExistsException(override val msg: String) extends ZookeeperException(msg)
case class NotEmptyException(override val msg: String) extends ZookeeperException(msg)
case class SessionExpiredException(override val msg: String) extends ZookeeperException(msg)
case class InvalidCallbackException(override val msg: String) extends ZookeeperException(msg)
case class InvalidAclException(override val msg: String) extends ZookeeperException(msg)
case class AuthFailedException(override val msg: String) extends ZookeeperException(msg)
case class SessionMovedException(override val msg: String) extends ZookeeperException(msg)
case class NotReadOnlyException(override val msg: String) extends ZookeeperException(msg)

case class ZKExc(override val msg: String) extends ZookeeperException(msg)
case class ZkDispatchingException(msg: String) extends RuntimeException(msg)

object ZookeeperException {
  def create(msg: String): ZookeeperException = new ZKExc(msg)
  def create(msg: String, cause: Throwable): Throwable = new ZKExc(msg).initCause(cause)

  def create(msg: String, code: Int): ZookeeperException = code match {
    case -1 => new SystemErrorException(msg + " " + errorCode.getError(code))
    case -2 => new RuntimeInconsistencyException(msg + " " + errorCode.getError(code))
    case -3 => new DataInconsistencyException(msg + " " + errorCode.getError(code))
    case -4 => new ConnectionLossException(msg + " " + errorCode.getError(code))
    case -5 => new MarshallingErrorException(msg + " " + errorCode.getError(code))
    case -6 => new UnimplementedException(msg + " " + errorCode.getError(code))
    case -7 => new OperationTimeoutException(msg + " " + errorCode.getError(code))
    case -8 => new BadArgumentsException(msg + " " + errorCode.getError(code))
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
  }

  def create(code: Int): ZookeeperException = create("", code)
}