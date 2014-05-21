package com.twitter.finagle.exp.zookeeper.watcher

import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.exp.zookeeper.transport.BufferReader
import com.twitter.finagle.exp.zookeeper.{ WatcherEvent, ReplyHeader}

/**
 * WatchManager may be used to manage watcher events, keep a Set of current watches.
 */
class WatchManager {
  var watchers: Set[Watcher] = Set[Watcher]()
}

object WatchManager {

  def decode(buffer: ChannelBuffer)= {
    val bw = BufferReader(buffer)

    val replyHeader = ReplyHeader.decode(bw)
    val watcherEventBody = new WatcherEvent(bw.readInt, bw.readInt, bw.readString)
  }
}