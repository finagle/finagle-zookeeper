package com.twitter.finagle.exp.zookeeper.watch

import com.twitter.finagle.exp.zookeeper.WatchEvent
import com.twitter.util.Promise
import scala.collection.mutable

// todo improve watch
/**
 * WatchManager is used to manage watches by keeping in memory the map
 * of active watches.
 *
 * @param chroot default user chroot
 */
private[finagle] class WatchManager(chroot: String, autoWatchReset: Boolean) {
  val dataWatches: mutable.HashMap[String, Promise[WatchEvent]] =
    mutable.HashMap()
  val existsWatches: mutable.HashMap[String, Promise[WatchEvent]] =
    mutable.HashMap()
  val childWatches: mutable.HashMap[String, Promise[WatchEvent]] =
    mutable.HashMap()

  def getDataWatches = this.synchronized(dataWatches)
  def getExistsWatches = this.synchronized(existsWatches)
  def getChildWatches = this.synchronized(childWatches)

  /**
   * Should register a new watch in the corresponding map
   *
   * @param map HashMap[String, Promise[WatchEvent]
   * @param path watch's path
   * @return a new Promise[WatchEvent]
   */
  private[this] def addWatch(
    map: mutable.HashMap[String, Promise[WatchEvent]],
    path: String): Promise[WatchEvent] = {

    map synchronized {
      map.getOrElse(path, {
        val p = Promise[WatchEvent]()
        map += path -> p
        p
      })
    }
  }

  /**
   * Should clear all watches
   */
  // fixme interrompre les watches au lieu de les supprimer
  def clearWatches() {
    this.synchronized {
      dataWatches.clear()
      existsWatches.clear()
      childWatches.clear()
    }
  }

  /**
   * Should find a watch and satisfy its promise with the event
   *
   * @param map the path -> watch map
   * @param event a watched event
   */
  private[this] def findAndSatisfy(
    map: mutable.HashMap[String, Promise[WatchEvent]],
    event: WatchEvent) {

    map synchronized {
      map.get(event.path) match {
        case Some(promise) =>
          promise.setValue(event)
          map -= event.path
        case None =>
      }
    }
  }

  /**
   * Should register a new watch
   *
   * @param path the node path
   * @param watchType watch type
   * @return Promise[WatchEvent]
   */
  def register(path: String, watchType: Int): Promise[WatchEvent] = {
    watchType match {
      case Watch.Type.data => addWatch(dataWatches, path)
      case Watch.Type.exists => addWatch(existsWatches, path)
      case Watch.Type.child => addWatch(childWatches, path)
    }
  }

  /**
   * We use this to process every watches events that comes in
   *
   * @param watchEvent the watch event that was received
   * @return Unit
   */
  def process(watchEvent: WatchEvent) {
    val event = WatchEvent(
      watchEvent.typ,
      watchEvent.state,
      watchEvent.path.substring(chroot.length))

    event.typ match {
      case Watch.EventType.NONE =>
        if (event.state != Watch.EventState.SYNC_CONNECTED)
          if (!autoWatchReset) clearWatches()

      case Watch.EventType.NODE_CREATED | Watch.EventType.NODE_DATA_CHANGED =>
        findAndSatisfy(dataWatches, event)
        findAndSatisfy(existsWatches, event)

      case Watch.EventType.NODE_DELETED =>
        findAndSatisfy(dataWatches, event)
        findAndSatisfy(existsWatches, event)
        findAndSatisfy(childWatches, event)

      case Watch.EventType.NODE_CHILDREN_CHANGED =>
        findAndSatisfy(childWatches, event)

      case _ => throw new RuntimeException(
        "Unsupported Watch.EventType came during WatchedEvent processing")
    }
  }
}