package com.twitter.finagle.exp.zookeeper.client

import com.twitter.util.Duration
import com.twitter.finagle.util.DefaultTimer


class PingTimer {

  val timer = DefaultTimer

  def apply(period: Duration)(f: => Unit) {
    timer.twitter.schedule(period)(f)
  }

  def stopTimer = {
    timer.twitter.stop()
  }

  def updateTimer(period: Duration)(f: => Unit) = {
    stopTimer
    apply(period)(f)
  }

}

