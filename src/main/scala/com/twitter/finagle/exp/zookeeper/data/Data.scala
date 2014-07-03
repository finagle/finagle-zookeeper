package com.twitter.finagle.exp.zookeeper.data

import com.twitter.finagle.exp.zookeeper.ZkDecodingException
import com.twitter.io.Buf
import com.twitter.util.{Throw, Return, Try}

private[finagle] trait Data {
  def buf: Buf
}

private[finagle] trait DataDecoder[T <: Data] {
  def unapply(buffer: Buf): Option[(T, Buf)]
  def apply(buffer: Buf): Try[(T, Buf)] = unapply(buffer) match {
    case Some((rep, rem)) => Return((rep, rem))
    case None => Throw(ZkDecodingException("Error while decoding data"))
  }
}