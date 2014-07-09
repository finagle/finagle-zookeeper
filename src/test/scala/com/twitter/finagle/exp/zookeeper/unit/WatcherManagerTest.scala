package com.twitter.finagle.exp.zookeeper.unit

import com.twitter.finagle.exp.zookeeper.WatchEvent
import com.twitter.finagle.exp.zookeeper.watcher.{Watch, WatcherManager}
import com.twitter.util.Await
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class WatcherManagerTest extends FunSuite {
  trait HelperTrait {
    val watchMngr = new WatcherManager("", false)
    val watchEvent = WatchEvent(Watch.EventType.NODE_DATA_CHANGED
      , Watch.EventState.SYNC_CONNECTED, "/zookeeper/test")
  }

  test("should register some watchers") {
    new HelperTrait {
      watchMngr.registerWatcher("/zookeeper/test", Watch.WatcherMapType.data)
      watchMngr.registerWatcher("/zookeeper/test", Watch.WatcherMapType.children)
      watchMngr.registerWatcher("/zookeeper/test", Watch.WatcherMapType.exists)
      assert(watchMngr.getDataWatchers.contains("/zookeeper/test"))
      assert(watchMngr.getChildrenWatchers.contains("/zookeeper/test"))
      assert(watchMngr.getExistsWatchers.contains("/zookeeper/test"))
    }
  }

  test("should register a watcher and trigger") {
    new HelperTrait {
      val watcher = watchMngr.registerWatcher(
        "/zookeeper/test", Watch.WatcherMapType.data)
      assert(watchMngr.getDataWatchers.contains("/zookeeper/test"))
      watchMngr.process(watchEvent)
      val rep = Await.result(watcher.event)
      assert(rep.path === "/zookeeper/test")
      assert(rep.state === Watch.EventState.SYNC_CONNECTED)
    }
  }

  test("should register a watcher and trigger with a chroot path") {
    new HelperTrait {
      override val watchMngr = new WatcherManager("/zookeeper", false)
      val watcher = watchMngr.registerWatcher(
        "/test", Watch.WatcherMapType.data)
      assert(watchMngr.getDataWatchers.contains("/test"))
      watchMngr.process(watchEvent)
      val rep = Await.result(watcher.event)
      assert(rep.path === "/test")
      assert(rep.state === Watch.EventState.SYNC_CONNECTED)
    }
  }

  test("should register a watcher and call isDefined") {
    new HelperTrait {
      val watcher = watchMngr.registerWatcher(
        "/zookeeper/test", Watch.WatcherMapType.data)
      assert(watchMngr.isWatcherDefined(watcher))
    }
  }

  test("should remove a watcher and call isDefined") {
    new HelperTrait {
      val watcher = watchMngr.registerWatcher(
        "/zookeeper/test", Watch.WatcherMapType.data)
      assert(watchMngr.isWatcherDefined(watcher))
      watchMngr.removeWatcher(watcher)
      assert(!watchMngr.isWatcherDefined(watcher))
    }
  }

  test("should remove some watchers and call isDefined") {
    new HelperTrait {
      val watcher = watchMngr.registerWatcher(
        "/zookeeper/test", Watch.WatcherMapType.data)
      val watcher2 = watchMngr.registerWatcher(
        "/zookeeper/test", Watch.WatcherMapType.children)
      val watcher3 = watchMngr.registerWatcher(
        "/zookeeper/test", Watch.WatcherMapType.exists)
      assert(watchMngr.isWatcherDefined(watcher))
      assert(watchMngr.isWatcherDefined(watcher2))
      assert(watchMngr.isWatcherDefined(watcher3))
      watchMngr.removeWatchers("/zookeeper/test", Watch.WatcherType.CHILDREN)
      assert(!watchMngr.isWatcherDefined(watcher2))
      watchMngr.removeWatchers("/zookeeper/test", Watch.WatcherType.DATA)
      assert(!watchMngr.isWatcherDefined(watcher))
      assert(!watchMngr.isWatcherDefined(watcher3))

      val watcher4 = watchMngr.registerWatcher(
        "/zookeeper/test", Watch.WatcherMapType.data)
      val watcher5 = watchMngr.registerWatcher(
        "/zookeeper/test", Watch.WatcherMapType.children)
      val watcher6 = watchMngr.registerWatcher(
        "/zookeeper/test", Watch.WatcherMapType.exists)
      watchMngr.removeWatchers("/zookeeper/test", Watch.WatcherType.ANY)
      assert(!watchMngr.isWatcherDefined(watcher4))
      assert(!watchMngr.isWatcherDefined(watcher5))
      assert(!watchMngr.isWatcherDefined(watcher6))
    }
  }
}
