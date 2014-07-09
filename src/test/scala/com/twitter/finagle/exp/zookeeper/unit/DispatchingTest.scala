package com.twitter.finagle.exp.zookeeper.unit

import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.OpCode
import com.twitter.finagle.exp.zookeeper._
import com.twitter.finagle.exp.zookeeper.client.ZkDispatcher
import com.twitter.finagle.exp.zookeeper.connection.ConnectionManager
import com.twitter.finagle.exp.zookeeper.session.SessionManager
import com.twitter.finagle.exp.zookeeper.transport.BufString
import com.twitter.finagle.exp.zookeeper.watcher.{Watch, WatcherManager}
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{CancelledRequestException, ChannelClosedException}
import com.twitter.io.Buf
import com.twitter.util.{Await, Future, Promise}
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class DispatchingTest extends FunSuite with MockitoSugar {
  trait TestHelper {
    val deleteReq = ReqPacket(
      Some(RequestHeader(3, OpCode.DELETE)),
      Some(DeleteRequest("/zookeeper/worker1/job1", 2))
    )
    val deleteRep = Future(
      Buf.Empty
        .concat(Buf.U32BE(3))
        .concat(Buf.U64BE(124))
        .concat(Buf.U32BE(0))
    )
    val notifRep = Future(
      Buf.Empty
        .concat(Buf.U32BE(-1))
        .concat(Buf.U64BE(134))
        .concat(Buf.U32BE(0))
        .concat(Buf.U32BE(Watch.EventType.NODE_CREATED))
        .concat(Buf.U32BE(Watch.EventState.SYNC_CONNECTED))
        .concat(BufString("/zookeeper"))
    )

    val transport = mock[Transport[Buf, Buf]]
    val dispatcher = Mockito.spy(new ZkDispatcher(transport))

    val connectionManager = new ConnectionManager(
      "127.0.0.1:2181",
      false,
      None,
      None)
    val sessionManager = new SessionManager(false)
    val watchManager: WatcherManager = new WatcherManager("", false)
    val configDispatcher = ReqPacket(None, Some(ConfigureRequest(
      connectionManager,
      sessionManager,
      watchManager
    )))
  }

  test("Basic req dispatching") {
    new TestHelper {
      Await.ready(dispatcher(configDispatcher))

      when(transport.write(any[Buf])) thenReturn Future.Done
      when(transport.read()) thenReturn deleteRep thenReturn Promise[Buf]()

      assert(Await.result(dispatcher(deleteReq)) == RepPacket(Some(0), None))
      verify(dispatcher, times(2)).apply(any[ReqPacket])
      verify(transport).write(any[Buf])
    }
  }

  test("Basic notification dispatching") {
    new TestHelper {
      when(transport.write(any[Buf])) thenReturn Future.Done
      when(transport.read()) thenReturn deleteRep thenReturn notifRep thenReturn
        Promise[Buf]()

      val watcher = watchManager.registerWatcher("/zookeeper", Watch.WatcherMapType.data)
      Await.ready(dispatcher(configDispatcher))
      assert(Await.result(dispatcher(deleteReq)) == RepPacket(Some(0), None))

      val rep = Await.result(watcher.event)
      assert(rep.path === "/zookeeper")
      assert(rep.state === Watch.EventState.SYNC_CONNECTED)
      verify(transport, times(3)).read()
      verify(dispatcher, times(2)).apply(any[ReqPacket])
    }
  }

  test("Request and notifications dispatching") {
    new TestHelper {
      Await.ready(dispatcher(configDispatcher))

      val watcher = watchManager.registerWatcher("/zookeeper", Watch.WatcherMapType.data)
      when(transport.write(any[Buf])) thenReturn Future.Done
      when(transport.read()) thenReturn deleteRep thenReturn notifRep thenReturn
        Promise[Buf]()

      assert(Await.result(dispatcher(deleteReq)) == RepPacket(Some(0), None))
      val rep = Await.result(watcher.event)
      assert(rep.path === "/zookeeper")
      assert(rep.state === Watch.EventState.SYNC_CONNECTED)
    }
  }

  test("multi requests with errors") {
    new TestHelper {
      Await.ready(dispatcher(configDispatcher))
      val wrongRep = Future(
        Buf.Empty
          .concat(Buf.U32BE(16))
          .concat(Buf.U64BE(124))
          .concat(Buf.U32BE(0))
      )
      when(transport.write(any[Buf])) thenReturn Future.Done
      when(transport.read()) thenReturn deleteRep thenReturn wrongRep thenReturn
        Promise[Buf]()
      assert(Await.result(dispatcher(deleteReq)) == RepPacket(Some(0), None))
    }
  }

  test("request should fail the dispatcher when writing") {
    new TestHelper {
      Await.ready(dispatcher(configDispatcher))
      when(transport.write(any[Buf])) thenReturn {
        Future.exception(new ChannelClosedException())
      }
      intercept[CancelledRequestException] {
        Await.result(dispatcher(deleteReq))
      }
    }
  }

  test("request should fail the dispatcher when reading") {
    new TestHelper {
      Await.ready(dispatcher(configDispatcher))
      when(transport.write(any[Buf])) thenReturn Future.Done
      when(transport.read()) thenReturn {
        Future.exception(new ChannelClosedException())
      }
      intercept[CancelledRequestException] {
        Await.result(dispatcher(deleteReq))
      }
    }
  }
}
