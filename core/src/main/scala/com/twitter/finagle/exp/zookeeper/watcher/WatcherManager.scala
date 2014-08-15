package com.twitter.finagle.exp.zookeeper.watcher

import com.twitter.finagle.exp.zookeeper.WatchEvent
import com.twitter.finagle.exp.zookeeper.client.ZkClient
import com.twitter.finagle.exp.zookeeper.watcher.Watch.WatcherMapType
import com.twitter.util.Promise
import scala.collection.mutable

/**
 * WatcherManager is used to manage watches by keeping in memory the map
 * of active watches.
 *
 * @param chroot default user chrooted path
 * @param autoWatchReset reset watches on reconnection
 */
private[finagle] class WatcherManager(
  chroot: String,
  autoWatchReset: Boolean
) {
  val dataWatchers: mutable.HashMap[String, Set[Watcher]] =
    mutable.HashMap()
  val existsWatchers: mutable.HashMap[String, Set[Watcher]] =
    mutable.HashMap()
  val childrenWatchers: mutable.HashMap[String, Set[Watcher]] =
    mutable.HashMap()

  def getDataWatchers = this.synchronized(dataWatchers)
  def getExistsWatchers = this.synchronized(existsWatchers)
  def getChildrenWatchers = this.synchronized(childrenWatchers)

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
      val watchers = map.getOrElse(path, Set.empty[Watcher])
      map += path -> (watchers + watcher)
      watcher
    }
  }

  /**
   * Should clear all watches
   */
  def clearWatchers() {
    this.synchronized {
      dataWatchers.clear()
      existsWatchers.clear()
      childrenWatchers.clear()
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
        childrenWatchers.synchronized {
          childrenWatchers.get(path).isDefined
        }

      case Watch.WatcherType.DATA =>
        dataWatchers.synchronized {
          dataWatchers.get(path).isDefined
        } || existsWatchers.synchronized {
          existsWatchers.get(path).isDefined
        }

      case Watch.WatcherType.ANY =>
        childrenWatchers.synchronized {
          childrenWatchers.get(path).isDefined
        } || dataWatchers.synchronized {
          dataWatchers.get(path).isDefined
        } || existsWatchers.synchronized {
          existsWatchers.get(path).isDefined
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
        dataWatchers.synchronized {
          dataWatchers.get(watcher.path).isDefined
        }
      case Watch.WatcherMapType.exists =>
        existsWatchers.synchronized {
          existsWatchers.get(watcher.path).isDefined
        }
      case Watch.WatcherMapType.`children` =>
        childrenWatchers.synchronized {
          childrenWatchers.get(watcher.path).isDefined
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
    val path: String = if (chroot.length > 0) {
      val serverPath = watchEvent.path
      if (serverPath == chroot) "/"
      else if (serverPath.length > chroot.length)
        watchEvent.path.substring(chroot.length)
      else {
        ZkClient.logger.warning("Got server path " + watchEvent.path
          + " which is too short for chroot path " + chroot)
        watchEvent.path
      }
    } else watchEvent.path

    val event = WatchEvent(
      watchEvent.typ,
      watchEvent.state,
      path)

    event.typ match {
      case Watch.EventType.NONE =>
        if (event.state != Watch.EventState.SYNC_CONNECTED)
          if (!autoWatchReset) clearWatchers()

      case Watch.EventType.NODE_CREATED | Watch.EventType.NODE_DATA_CHANGED =>
        findAndSatisfy(dataWatchers, event)
        findAndSatisfy(existsWatchers, event)

      case Watch.EventType.NODE_DELETED =>
        findAndSatisfy(dataWatchers, event)
        findAndSatisfy(existsWatchers, event)
        findAndSatisfy(childrenWatchers, event)

      case Watch.EventType.NODE_CHILDREN_CHANGED =>
        findAndSatisfy(childrenWatchers, event)

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
      case Watch.WatcherMapType.data =>
        addWatcher(dataWatchers, WatcherMapType.data, path)
      case Watch.WatcherMapType.exists =>
        addWatcher(existsWatchers, WatcherMapType.exists, path)
      case Watch.WatcherMapType.`children` =>
        addWatcher(childrenWatchers, WatcherMapType.children, path)
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
      path: String
    ) {
      map synchronized {
        map.remove(path)
      }
    }

    if (!isWatcherDefined(path, watcherType))
      throw new IllegalArgumentException("No watch registered for this node")

    watcherType match {
      case Watch.WatcherType.CHILDREN =>
        removeAllWatchers(childrenWatchers, path)

      case Watch.WatcherType.DATA =>
        removeAllWatchers(dataWatchers, path)
        removeAllWatchers(existsWatchers, path)

      case Watch.WatcherType.ANY =>
        removeAllWatchers(childrenWatchers, path)
        removeAllWatchers(dataWatchers, path)
        removeAllWatchers(existsWatchers, path)
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
      case Watch.WatcherMapType.data =>
        removeFromMap(dataWatchers, watcher)
      case Watch.WatcherMapType.exists =>
        removeFromMap(existsWatchers, watcher)
      case Watch.WatcherMapType.children =>
        removeFromMap(childrenWatchers, watcher)
    }
  }
}