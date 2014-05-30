package com.twitter.finagle.exp.zookeeper.watcher

import com.twitter.finagle.exp.zookeeper.WatcherEvent
import scala.collection.mutable
import com.twitter.util.Promise

/**
 * WatchManager may be used to manage watcher events, keep a Set of current watches.
 */
class WatchManager {
  private val dataWatches: mutable.HashMap[String, Promise[WatcherEvent]] = mutable.HashMap()
  private val existsWatches: mutable.HashMap[String, Promise[WatcherEvent]] = mutable.HashMap()
  private val childWatches: mutable.HashMap[String, Promise[WatcherEvent]] = mutable.HashMap()

  def getDataWatches = this.synchronized(dataWatches)
  def getExistsWatches = this.synchronized(existsWatches)
  def getChildWatches = this.synchronized(childWatches)

  def register(path: String, typ: Int) = {

    /**
     * 1/ Check if the watch is correctly formed
     * 2/ Check if the current watch does not already exists (same path and type)
     * 3/ If yes check if auto reset AND promise already satisfied (means this a watch refresh) then renew the promise
     * 4/ If no add it to the corresponding map
     */

    typ match {
      case WatchType.data => dataWatches += path -> Promise[WatcherEvent]()
      case WatchType.exists => path -> Promise[WatcherEvent]()
      case WatchType.child => path -> Promise[WatcherEvent]()
    }
  }

  def process(event: WatcherEvent) = {
    event.typ match {
      case eventType.NONE => // TODO
      case eventType.NODE_CREATED =>
        dataWatches synchronized {
          dataWatches -= event.path
        }
        existsWatches synchronized {
          existsWatches -= event.path
        }

      case eventType.NODE_DATA_CHANGED =>
        dataWatches synchronized {
          dataWatches -= event.path
        }
        existsWatches synchronized {
          existsWatches -= event.path
        }

      case eventType.NODE_DELETED =>
        dataWatches synchronized {
          dataWatches -= event.path
        }

        existsWatches synchronized {
          existsWatches -= event.path
        }

        childWatches synchronized {
          childWatches -= event.path
        }
      case eventType.NODE_CHILDREN_CHANGED =>
        childWatches synchronized {
          childWatches -= event.path
        }
      case _ => throw new RuntimeException("This Watch event is not supported")
    }
  }
}

object WatchType {
  val data = 1
  val exists = 2
  val child = 3
}