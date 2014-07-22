package com.twitter.finagle.exp.zookeeper.client

import com.twitter.finagle.Stack
import com.twitter.util.Duration
import com.twitter.util.TimeConversions._

object Params {
  case class ZkConfiguration(
    autoWatchReset: Boolean,
    canReadOnly: Boolean,
    chroot: String,
    sessionTimeout: Duration
  )
  implicit object ZkConfiguration extends Stack.Param[ZkConfiguration] {
    def default: ZkConfiguration =
      ZkConfiguration(true, true, "", 3000.milliseconds)
  }

  case class AutoReconnect(
    autoReconnect: Boolean,
    autoRwServerSearch: Option[Duration],
    preventiveSearch: Option[Duration],
    timeBetweenAttempts: Option[Duration],
    timeBetweenLinkCheck: Option[Duration],
    maxConsecutiveRetries: Int,
    maxReconnectAttempts: Int
  )
  implicit object AutoReconnect extends Stack.Param[AutoReconnect] {
    def default: AutoReconnect = AutoReconnect(
      autoReconnect = false,
      autoRwServerSearch = None,
      preventiveSearch = None,
      timeBetweenAttempts = None,
      timeBetweenLinkCheck = None,
      maxConsecutiveRetries = 1,
      maxReconnectAttempts = 1
    )
  }
}