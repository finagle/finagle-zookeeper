package com.twitter.finagle.exp.zookeeper

class ZookeeperException(msg: String) extends RuntimeException(msg)

object ZookeeperException {
  def create(msg: String): ZookeeperException = new ZookeeperException(msg)
  def create(msg: String, errorCode: Int): Throwable = {
    val err = ZookeeperDefinitions.errorCode.getError(errorCode)
    val cause = new Exception(err)
    create(msg, cause)
  }
  def create(msg: String, cause: Throwable): Throwable = new ZookeeperException(msg).initCause(cause)
}