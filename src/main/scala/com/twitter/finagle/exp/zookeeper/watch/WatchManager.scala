package com.twitter.finagle.exp.zookeeper.watch

import com.twitter.finagle.exp.zookeeper.WatchEvent
import scala.collection.mutable
import com.twitter.util.Promise

/**
 * WatchManager may be used to manage watcher events, keep a Set of current watches.
 */

class WatchManager {
  private val dataWatches: mutable.HashMap[String, Promise[WatchEvent]] = mutable.HashMap()
  private val existsWatches: mutable.HashMap[String, Promise[WatchEvent]] = mutable.HashMap()
  private val childWatches: mutable.HashMap[String, Promise[WatchEvent]] = mutable.HashMap()

  def getDataWatches = this.synchronized(dataWatches)
  def getExistsWatches = this.synchronized(existsWatches)
  def getChildWatches = this.synchronized(childWatches)

  def register(path: String, watchType: Int): Promise[WatchEvent] = {
    println("Registering " + path)

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
   * @param event the watch event that was received
   * @return
   */
  def process(event: WatchEvent) = {
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
      case _ => throw new RuntimeException("This Watch event is not supported")
    }
  }

  /**
   * Just to know if we are currently waiting for some events
   * @return if the watch manager has some watches
   */
  def hasWatches: Boolean = {
    if (!dataWatches.keySet.isEmpty || !existsWatches.keySet.isEmpty
      || !childWatches.keySet.isEmpty)
      true
    else false
  }
}

object WatchType {
  val data = 1
  val exists = 2
  val child = 3
}