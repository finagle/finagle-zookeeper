package com.twitter.finagle.exp.zookeeper.watcher

import com.twitter.finagle.exp.zookeeper.WatchEvent
import com.twitter.finagle.exp.zookeeper.watcher.Watch.WatcherMapType
import com.twitter.util.Promise
import scala.collection.mutable

/**
 * WatcherManager is used to manage watches by keeping in memory the map
 * of active watches.
 *
 * @param chroot default user chroot
 */
private[finagle] class WatcherManager(chroot: String, autoWatchReset: Boolean) {
  val dataWatches: mutable.HashMap[String, Set[Watcher]] =
    mutable.HashMap()
  val existsWatches: mutable.HashMap[String, Set[Watcher]] =
    mutable.HashMap()
  val childrenWatches: mutable.HashMap[String, Set[Watcher]] =
    mutable.HashMap()

  def getDataWatchers = this.synchronized(dataWatches)
  def getExistsWatchers = this.synchronized(existsWatches)
  def getChildrenWatchers = this.synchronized(childrenWatches)

  /**
   * Should register a new watcher in the corresponding map
   *
   * @param map HashMap[String, Promise[WatchEvent]
   * @param path watcher's path
   * @return a new Watcher
   */
  private[this] def addWatcher(
    map: mutable.HashMap[String, Set[Watcher]],
    typ: Int,
    path: String
    ): Watcher = {
    val watcher = new Watcher(path, typ, Promise[WatchEvent]())

    map synchronized {
      val watchers = map.getOrElse(path, {
        Set.empty[Watcher]
      })
      map += path -> (watchers + watcher)
      watcher
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

  /**
   * Checks if a Watcher is still defined in the watcher manager
   *
   * @param path the path of the znode
   * @param watcherType the type of the watcher
   * @return Whether or not the watcher is still defined
   */
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
   * Checks if a Watcher is still defined in the watcher manager
   *
   * @param watcher the watcher to test
   * @return Whether or not the watcher is still defined
   */
  def isWatcherDefined(watcher: Watcher): Boolean = {
    watcher.typ match {
      case Watch.WatcherMapType.data =>
        dataWatches.synchronized {
          dataWatches.get(watcher.path).isDefined
        }
      case Watch.WatcherMapType.exists =>
        existsWatches.synchronized {
          existsWatches.get(watcher.path).isDefined
        }
      case Watch.WatcherMapType.`children` =>
        childrenWatches.synchronized {
          childrenWatches.get(watcher.path).isDefined
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
    map: mutable.HashMap[String, Set[Watcher]],
    event: WatchEvent) {

    map synchronized {
      map.get(event.path) match {
        case Some(set) =>
          set map { watcher =>
            watcher.event.setValue(event)
          }
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
   * @param mapType request type: child, data, exists
   * @return a new Watcher
   */
  private[finagle] def registerWatcher(path: String, mapType: Int): Watcher = {
    mapType match {
      case Watch.WatcherMapType.data => addWatcher(dataWatches, WatcherMapType.data, path)
      case Watch.WatcherMapType.exists => addWatcher(existsWatches, WatcherMapType.exists, path)
      case Watch.WatcherMapType.`children` => addWatcher(childrenWatches, WatcherMapType.children, path)
    }
  }

  /**
   * Unregister watchers on a node by WatcherType.
   *
   * @param path node path
   * @param watcherType watcher type (children, data, any)
   */
  def removeWatchers(path: String, watcherType: Int) {
    def removeAllWatchers(
      map: mutable.HashMap[String, Set[Watcher]],
      path: String) {
      map synchronized {
        map.remove(path)
      }
    }

    if (!isWatcherDefined(path, watcherType))
      throw new IllegalArgumentException("No watch registered for this node")

    watcherType match {
      case Watch.WatcherType.CHILDREN =>
        removeAllWatchers(childrenWatches, path)

      case Watch.WatcherType.DATA =>
        removeAllWatchers(dataWatches, path)
        removeAllWatchers(existsWatches, path)

      case Watch.WatcherType.ANY =>
        removeAllWatchers(childrenWatches, path)
        removeAllWatchers(dataWatches, path)
        removeAllWatchers(existsWatches, path)
    }
  }

  /**
   * Remove a watcher from its map, it will never be satisfied.
   *
   * @param watcher the watcher to disable
   */
  def removeWatcher(watcher: Watcher) {
    def removeFromMap(
      map: mutable.HashMap[String, Set[Watcher]],
      watcher: Watcher
      ) {
      map synchronized {
        map.get(watcher.path) match {
          case Some(watchers) =>
            val newWatchers = watchers - watcher
            if (newWatchers.isEmpty) map.remove(watcher.path)
            else map += watcher.path -> (watchers - watcher)
          case None =>
        }
      }
    }

    watcher.typ match {
      case Watch.WatcherMapType.data => removeFromMap(dataWatches, watcher)
      case Watch.WatcherMapType.exists => removeFromMap(existsWatches, watcher)
      case Watch.WatcherMapType.children => removeFromMap(childrenWatches, watcher)
    }
  }
}