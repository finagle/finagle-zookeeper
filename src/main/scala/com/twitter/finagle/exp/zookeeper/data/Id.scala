package com.twitter.finagle.exp.zookeeper.data

import com.twitter.finagle.exp.zookeeper.data.ACL.Perms
import com.twitter.finagle.exp.zookeeper.transport.{BufArray, BufString}
import com.twitter.io.Buf

case class Id(scheme: String, data: String) extends Data {
  def buf: Buf = Buf.Empty
    .concat(BufString(scheme))
    .concat(BufString(data))
}
case class Auth(scheme: String, data: Array[Byte]) extends Data {
  def buf: Buf = Buf.Empty
    .concat(BufString(scheme))
    .concat(BufArray(data))
}

object Id extends DataDecoder[Id] {
  def unapply(buf: Buf): Option[(Id, Buf)] = buf match {
    case BufString(scheme, BufString(id, rem)) => Some(Id(scheme, id), rem)
    case _ => None
  }
}

/**
 * A set of basic ids
 */
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
  val OPEN_ACL_UNSAFE = Seq[ACL](ACL(Perms.ALL, ANYONE_ID_UNSAFE))

  /**
   * This ACL gives the creators authentication id's all permissions.
   */
  val CREATOR_ALL_ACL = Seq[ACL](ACL(Perms.ALL, AUTH_IDS))

  /**
   * This ACL gives the world the ability to read.
   */
  val READ_ACL_UNSAFE = Seq[ACL](ACL(Perms.READ, ANYONE_ID_UNSAFE))
}