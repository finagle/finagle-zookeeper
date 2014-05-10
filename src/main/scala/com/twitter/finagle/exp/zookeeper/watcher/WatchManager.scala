package com.twitter.finagle.exp.zookeeper.watcher

import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.exp.zookeeper.transport.BufferReader

class WatchManager {
  var watchers: Set[Watcher] = Set[Watcher]()
}

object WatchManager{

  def decode(buffer: ChannelBuffer) = {
    val bw = BufferReader(buffer)

    bw.readInt // packet size

    if (bw.readInt == -1) {
      bw.readLong
      bw.readInt

      val _type = eventType.getEvent(bw.readInt)
      val _state = zkState.getState(bw.readInt)

      println("Watch event | type: " + _type + " | state: " + _state + " | path: " + bw.readString)
    }
  }
}