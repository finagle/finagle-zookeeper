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

  private val pendingWatches: mutable.HashMap[Int, (String, Int)] = mutable.HashMap()

  def getDataWatches = this.synchronized(dataWatches)
  def getExistsWatches = this.synchronized(existsWatches)
  def getChildWatches = this.synchronized(childWatches)

  def prepareRegister(path: String, watchType: Int, xid: Int) = {
    println("Preparing %s %d %d".format(path, watchType, xid))
    pendingWatches.get(xid) match {
      case Some(res) => throw new RuntimeException("XID already exists for a new watch! impossible")
      case None =>
        pendingWatches += xid ->(path, watchType)
    }
  }

  def register(xid: Int): Option[Promise[WatcherEvent]] = {
    println("Registering " + xid)
    if (pendingWatches.isDefinedAt(xid)) {
      val (path, watchType) = pendingWatches.get(xid) match {
        case Some(res) =>
          pendingWatches -= xid
          res
        case None => throw new RuntimeException("This watch is not prepared! impossible")
      }

      watchType match {
        case WatchType.data =>
          dataWatches.get(path) match {
            case Some(promise) => Some(promise)
            case None =>
              val p = Promise[WatcherEvent]()
              dataWatches += path -> p
              Some(p)
          }

        case WatchType.exists =>
          existsWatches.get(path) match {
            case Some(promise) => Some(promise)
            case None =>
              val p = Promise[WatcherEvent]()
              existsWatches += path -> p
              Some(p)
          }
        case WatchType.child =>
          childWatches.get(path) match {
            case Some(promise) => Some(promise)
            case None =>
              val p = Promise[WatcherEvent]()
              childWatches += path -> p
              Some(p)
          }
      }
    } else {
      None
    }
  }

  def process(event: WatcherEvent) = {
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
}

object WatchType {
  val data = 1
  val exists = 2
  val child = 3
}