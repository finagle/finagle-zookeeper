package com.twitter.finagle.exp.zookeeper.watch

import com.twitter.finagle.exp.zookeeper.WatchEvent
import com.twitter.util.Promise
import scala.collection.mutable

/**
 * WatchManager may be used to manage watcher events, keep a Set of current watches.
 */

class WatchManager(chroot: String) {
  private val dataWatches: mutable.HashMap[String, Promise[WatchEvent]] = mutable.HashMap()
  private val existsWatches: mutable.HashMap[String, Promise[WatchEvent]] = mutable.HashMap()
  private val childWatches: mutable.HashMap[String, Promise[WatchEvent]] = mutable.HashMap()

  def getDataWatches = this.synchronized(dataWatches)
  def getExistsWatches = this.synchronized(existsWatches)
  def getChildWatches = this.synchronized(childWatches)

  private[finagle] def register(path: String, watchType: Int): Promise[WatchEvent] = {
    watchType match {
      case WatchType.data =>
        dataWatches.get(path) match {
          case Some(promise) => promise
          case None =>
            val p = Promise[WatchEvent]()
            dataWatches += path -> p
            p
        }

      case WatchType.exists =>
        existsWatches.get(path) match {
          case Some(promise) => promise
          case None =>
            val p = Promise[WatchEvent]()
            existsWatches += path -> p
            p
        }
      case WatchType.child =>
        childWatches.get(path) match {
          case Some(promise) => promise
          case None =>
            val p = Promise[WatchEvent]()
            childWatches += path -> p
            p
        }
    }
  }

  /**
   * We use this to process every watches events that comes in
   * @param watchEvent the watch event that was received
   * @return
   */
  private[finagle] def process(watchEvent: WatchEvent) = {
    val event = WatchEvent(
      watchEvent.typ,
      watchEvent.state,
      watchEvent.path.substring(chroot.length))
    event.typ match {
      case eventType.NONE => // TODO
      case eventType.NODE_CREATED =>
        dataWatches synchronized {
          dataWatches.get(event.path) match {
            case Some(promise) =>
              promise.setValue(event)
              dataWatches -= event.path
            case None =>
          }
        }

        existsWatches synchronized {
          existsWatches.get(event.path) match {
            case Some(promise) =>
              promise.setValue(event)
              existsWatches -= event.path
            case None =>
          }
        }

      case eventType.NODE_DATA_CHANGED =>
        dataWatches synchronized {
          dataWatches.get(event.path) match {
            case Some(promise) =>
              promise.setValue(event)
              dataWatches -= event.path
            case None =>
          }
        }

        existsWatches synchronized {
          existsWatches.get(event.path) match {
            case Some(promise) =>
              promise.setValue(event)
              existsWatches -= event.path
            case None =>
          }
        }

      case eventType.NODE_DELETED =>
        dataWatches synchronized {
          dataWatches.get(event.path) match {
            case Some(promise) =>
              promise.setValue(event)
              dataWatches -= event.path
            case None =>
          }
        }

        existsWatches synchronized {
          existsWatches.get(event.path) match {
            case Some(promise) =>
              promise.setValue(event)
              existsWatches -= event.path
            case None =>
          }
        }

        childWatches synchronized {
          childWatches.get(event.path) match {
            case Some(promise) =>
              promise.setValue(event)
              childWatches -= event.path
            case None =>
          }
        }
      case eventType.NODE_CHILDREN_CHANGED =>
        childWatches synchronized {
          childWatches.get(event.path) match {
            case Some(promise) =>
              promise.setValue(event)
              childWatches -= event.path
            case None =>
          }
        }
      // fixme handle exception in a Try
      case _ => throw new RuntimeException("This Watch event is not supported")
    }
  }
}

object WatchType {
  val data = 1
  val exists = 2
  val child = 3
}