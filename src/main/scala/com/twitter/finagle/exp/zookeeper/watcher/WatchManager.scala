package com.twitter.finagle.exp.zookeeper.watcher

import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.exp.zookeeper.transport.BufferReader
import com.twitter.finagle.exp.zookeeper.{WatcherEventBody, WatcherEvent, ReplyHeader}

class WatchManager {
  var watchers: Set[Watcher] = Set[Watcher]()
}

object WatchManager {

  def decode(buffer: ChannelBuffer)= {
    val bw = BufferReader(buffer)

    val replyHeader = ReplyHeader.decode(bw)
    val watcherEventBody = new WatcherEventBody(bw.readInt, bw.readInt, bw.readString)
  }
}