package com.twitter.finagle.exp.zookeeper

/* Custom Zookeeper exception */
case class ZookeeperException(msg:String) extends RuntimeException(msg)
case class NoNodeException(override val msg: String) extends ZookeeperException(msg)
case class ZKExc(override val msg: String) extends ZookeeperException(msg)

object ZookeeperException {
  def create(msg: String): ZookeeperException = new ZKExc(msg)
  def create(msg: String, cause: Throwable): Throwable = new ZKExc(msg).initCause(cause)

  def create(msg:String, code: Int): ZookeeperException = code match {
    case -101 => new NoNodeException(msg + err)
  }

  def create(code: Int): ZookeeperException = create("", code)
}