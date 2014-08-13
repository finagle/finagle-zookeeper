package com.twitter.finagle.exp.zookeeper.integration.standalone.extra

import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.CreateMode
import com.twitter.finagle.exp.zookeeper.data.Ids
import com.twitter.finagle.exp.zookeeper.integration.standalone.StandaloneIntegrationConfig
import com.twitter.finagle.exp.zookeeper.watcher.Watch.{EventState, EventType}
import com.twitter.util.{Await, Future}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StressTest extends FunSuite with StandaloneIntegrationConfig {

  test("watcher test") {
    pending
    newClient()
    connect()

    val ret = Await.result {
      Future.collect {
        0 until 1000 map { i =>
          for {
            _ <- client.get.create(
              "/foo-" + i, ("foodata" + i).getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT
            )
            getdata <- client.get.getData("/foo-" + i, true)
            exists <- client.get.exists("/foo-" + i, true)
            setdata <- client.get.setData("/foo-" + i, ("foodata2-" + i).getBytes, -1)
            setdata2 <- client.get.setData("/foo-" + i, ("foodata3-" + i).getBytes, -1)
          } yield (getdata, exists, setdata, setdata2)
        }
      }
    }

    ret map { grpRep =>
      val (getdata, exists, stat, stat2) = grpRep
      assert(getdata.watcher.get.event.isDefined)
      assert(Await.result(getdata.watcher.get.event).typ === EventType.NODE_DATA_CHANGED)
      assert(Await.result(getdata.watcher.get.event).state === EventState.SYNC_CONNECTED)
      assert(exists.watcher.get.event.isDefined)
      assert(Await.result(exists.watcher.get.event).typ === EventType.NODE_DATA_CHANGED)
      assert(Await.result(exists.watcher.get.event).state === EventState.SYNC_CONNECTED)
    }

    Await.result {
      Future.collect {
        0 until 1000 map { i =>
          for {
            _ <- client.get.delete("/foo-" + i, -1)
          } yield None
        }
      }
    }

    disconnect()
    Await.result(client.get.close())
  }

  test("connect-disconnect test") {
    pending
    newClient()

    for (i <- 0 until 500) {
      connect()
      disconnect()
    }

    Await.ready(client.get.close())
  }

  test("change host test") {
    pending
    newClient()
    connect()

    for (i <- 0 until 500) {
      Await.result(client.get.changeHost())
    }

    disconnect()
    Await.ready(client.get.close())
  }
}
