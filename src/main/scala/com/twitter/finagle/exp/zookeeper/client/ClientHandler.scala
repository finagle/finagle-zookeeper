package com.twitter.finagle.exp.zookeeper.client

import com.twitter.finagle.Stack
import com.twitter.util.Duration
import com.twitter.util.TimeConversions._

case class ClientHandler(
  val autoReconnect: Boolean = true,
  val autoWatchReset: Boolean = true,
  val chroot: String = "",
  val sessionTimeout: Duration = 3000.milliseconds,
  val maxConsecutiveRetries: Option[Int] = Some(10),
  val maxReconnectAttempts: Option[Int] = Some(5),
  val timeBetweenAttempts: Option[Duration] = Some(30.seconds),
  val timeBetweenLinkCheck: Option[Duration] = Some(1000.milliseconds / 2),
  val timeBetweenRwSrch: Option[Duration] = Some(1.minute),
  val timeBetweenPrevSrch: Option[Duration] = Some(10.minutes),
  val canReadOnly: Boolean = false
  )

object ClientHandler {

  case class Chroot(path: String)
  implicit object Chroot extends Stack.Param[Chroot] {
    val default = Chroot("")
  }

  def apply(params: Stack.Params): ClientHandler = {
    val Chroot(chrt) = params[Chroot]
    ClientHandler(
      chroot = chrt
    )
  }
}
