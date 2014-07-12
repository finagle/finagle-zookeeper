package com.twitter.finagle.exp.zookeeper.client

import com.twitter.finagle.Stack
import com.twitter.util.Duration
import com.twitter.util.TimeConversions._

case class ClientHandler(
  autoReconnect: Boolean,
  autoWatchReset: Boolean,
  chroot: String,
  sessionTimeout: Duration,
  maxConsecutiveRetries: Option[Int],
  maxReconnectAttempts: Option[Int],
  timeBetweenAttempts: Option[Duration],
  timeBetweenLinkCheck: Option[Duration],
  timeBetweenRwSrch: Option[Duration],
  timeBetweenPrevSrch: Option[Duration],
  canReadOnly: Boolean
  )

object ClientHandler {
  case class AutoReconnect(
    autoReconnect: Boolean = false,
    timeBetweenAttempts: Option[Duration] = None,
    timeBetweenLinkCheck: Option[Duration] = None,
    maxConsecutiveRetries: Option[Int] = None,
    maxReconnectAttempts: Option[Int] = None
    )
  implicit object AutoReconnect extends Stack.Param[AutoReconnect] {
    def default: AutoReconnect = AutoReconnect()
  }

  case class AutoRwServerSearch(timeInterval: Option[Duration] = None)
  implicit object AutoRwServerSearch extends Stack.Param[AutoRwServerSearch] {
    def default: AutoRwServerSearch = AutoRwServerSearch()
  }

  case class AutoWatchReset(set: Boolean)
  implicit object AutoWatchReset extends Stack.Param[AutoWatchReset] {
    override def default: AutoWatchReset = AutoWatchReset(false)
  }

  case class CanReadOnly(activate: Boolean)
  implicit object CanReadOnly extends Stack.Param[CanReadOnly] {
    override def default: CanReadOnly = CanReadOnly(false)
  }

  case class Chroot(path: String)
  implicit object Chroot extends Stack.Param[Chroot] {
    val default = Chroot("")
  }

  case class PreventiveSearch(timeInterval: Option[Duration] = None)
  implicit object PreventiveSearch extends Stack.Param[PreventiveSearch] {
    override def default: PreventiveSearch = PreventiveSearch()
  }

  case class SessionTimeout(duration: Duration = 3000.milliseconds)
  implicit object SessionTimeout extends Stack.Param[SessionTimeout] {
    override def default: SessionTimeout = SessionTimeout()
  }

  def apply(params: Stack.Params): ClientHandler = {
    val AutoReconnect(autoreco, tba, tblc, mcr, mra) = params[AutoReconnect]
    val AutoRwServerSearch(tbrs) = params[AutoRwServerSearch]
    val AutoWatchReset(autoWatch) = params[AutoWatchReset]
    val CanReadOnly(canRO) = params[CanReadOnly]
    val Chroot(chrt) = params[Chroot]
    val PreventiveSearch(tbps) = params[PreventiveSearch]
    val SessionTimeout(sessTimeout) = params[SessionTimeout]

    ClientHandler(
      autoreco,
      autoWatch,
      chrt,
      sessTimeout,
      mcr,
      mra,
      tba,
      tblc,
      tbrs,
      tbps,
      canRO
    )
  }
}