package com.twitter.finagle.exp.zookeeper.utils

import com.twitter.finagle.exp.zookeeper.transport.Buffer

object Implicits {
  implicit def someToBuffer(buff: Option[Buffer]): Buffer = buff match {
    case Some(buff) => buff
    case None => Buffer.getDynamicBuffer(0)
  }

}
