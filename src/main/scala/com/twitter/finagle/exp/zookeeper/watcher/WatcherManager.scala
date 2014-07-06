package com.twitter.finagle.exp.zookeeper.watcher

import com.twitter.finagle.exp.zookeeper.WatchEvent
import com.twitter.util.Promise
import scala.collection.mutable

/**
 * WatcherManager is used to manage watches by keeping in memory the map
 * of active watches.
 *
 * @param chroot default user chroot
 */
private[finagle] class WatcherManager(chroot: String, autoWatchReset: Boolean) {
  val dataWatches: mutable.HashMap[String, Promise[WatchEvent]] =
    mutable.HashMap()
  val existsWatches: mutable.HashMap[String, Promise[WatchEvent]] =
    mutable.HashMap()
  val childrenWatches: mutable.HashMap[String, Promise[WatchEvent]] =
    mutable.HashMap()

  def getDataWatchers = this.synchronized(dataWatches)
  def getExistsWatchers = this.synchronized(existsWatches)
  def getChildWatchers = this.synchronized(childrenWatches)

  /**
   * Should register a new watcher in the corresponding map
   *
   * @param map HashMap[String, Promise[WatchEvent]
   * @param path watcher's path
   * @return a new Promise[WatchEvent]
   */
  private[this] def addWatcher(
    map: mutable.HashMap[String, Promise[WatchEvent]],
    path: String
    ): Promise[WatchEvent] = {

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
  def clearWatchers() {
    this.synchronized {
      dataWatches.clear()
      existsWatches.clear()
      childrenWatches.clear()
    }
  }

  private[this] def removeWatchers(
    map: mutable.HashMap[String, Promise[WatchEvent]],
    path: String) {
    map synchronized {
      map.get(path) match {
        case Some(promise) => map -= path
        case None =>
      }
    }
  }

  def isWatcherDefined(path: String, watcherType: Int): Boolean = {
    watcherType match {
      case Watch.WatcherType.CHILDREN =>
        childrenWatches.synchronized {
          childrenWatches.get(path).isDefined
        }

      case Watch.WatcherType.DATA =>
        dataWatches.synchronized {
          dataWatches.get(path).isDefined
        } || existsWatches.synchronized {
          existsWatches.get(path).isDefined
        }

      case Watch.WatcherType.ANY =>
        childrenWatches.synchronized {
          childrenWatches.get(path).isDefined
        } || dataWatches.synchronized {
          dataWatches.get(path).isDefined
        } || existsWatches.synchronized {
          existsWatches.get(path).isDefined
        }
    }
  }

  /**
   * Should find a watcher and satisfy its promise with the event
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
   * We use this to process every watches events that comes in
   *
   * @param watchEvent the watch event that was received
   * @return Unit
   */
  private[finagle] def process(watchEvent: WatchEvent) {
    val event = WatchEvent(
      watchEvent.typ,
      watchEvent.state,
      watchEvent.path.substring(chroot.length))

    event.typ match {
      case Watch.EventType.NONE =>
        if (event.state != Watch.EventState.SYNC_CONNECTED)
          if (!autoWatchReset) clearWatchers()

      case Watch.EventType.NODE_CREATED | Watch.EventType.NODE_DATA_CHANGED =>
        findAndSatisfy(dataWatches, event)
        findAndSatisfy(existsWatches, event)

      case Watch.EventType.NODE_DELETED =>
        findAndSatisfy(dataWatches, event)
        findAndSatisfy(existsWatches, event)
        findAndSatisfy(childrenWatches, event)

      case Watch.EventType.NODE_CHILDREN_CHANGED =>
        findAndSatisfy(childrenWatches, event)

      case _ => throw new RuntimeException(
        "Unsupported Watch.EventType came during WatchedEvent processing")
    }
  }

  /**
   * Should register a new watcher.
   *
   * @param path the node path
   * @param requestType request type: child, data, exists
   * @return Promise[WatchEvent]
   */
  private[finagle] def registerWatcher(path: String, requestType: Int): Promise[WatchEvent] = {
    requestType match {
      case Watch.RequestType.data => addWatcher(dataWatches, path)
      case Watch.RequestType.exists => addWatcher(existsWatches, path)
      case Watch.RequestType.`children` => addWatcher(childrenWatches, path)
    }
  }

  /**
   * Unregister watchers on a node.
   *
   * @param path node path
   * @param watcherType watcher type (children, data, any)
   * @since 3.5.0
   */
  def removeWatchers(path: String, watcherType: Int) {
    if (!isWatcherDefined(path, watcherType))
      throw new IllegalArgumentException("No watch registered for this node")

    watcherType match {
      case Watch.WatcherType.CHILDREN =>
        removeWatchers(childrenWatches, path)

      case Watch.WatcherType.DATA =>
        removeWatchers(dataWatches, path)
        removeWatchers(existsWatches, path)

      case Watch.WatcherType.ANY =>
        removeWatchers(childrenWatches, path)
        removeWatchers(dataWatches, path)
        removeWatchers(existsWatches, path)
    }
  }
}