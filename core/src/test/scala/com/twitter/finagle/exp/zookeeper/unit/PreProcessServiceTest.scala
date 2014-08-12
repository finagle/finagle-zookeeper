package com.twitter.finagle.exp.zookeeper.unit

import com.twitter.finagle.CancelledRequestException
import com.twitter.finagle.exp.zookeeper._
import com.twitter.finagle.exp.zookeeper.client.PreProcessService
import com.twitter.finagle.exp.zookeeper.client.managers.AutoLinkManager
import com.twitter.finagle.exp.zookeeper.connection.{Connection, ConnectionManager}
import com.twitter.finagle.exp.zookeeper.data.Ids
import com.twitter.finagle.exp.zookeeper.session.SessionManager
import com.twitter.util.{Await, Future}
import org.scalatest.FunSuite
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import org.mockito.Matchers._
import org.mockito.Mockito
import org.mockito.Mockito.{times, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class PreProcessServiceTest extends FunSuite with MockitoSugar {
  trait HelperTrait {
    val con = mock[Connection]
    val conMgnr = new ConnectionManager("127.0.0.1:2181", None, false, None, None)
    conMgnr.connection = Some(con)
    val sessMngr = new SessionManager(false)
    val autoMngr = mock[AutoLinkManager]
    val preProcess = Mockito.spy(new PreProcessService(conMgnr, sessMngr, autoMngr))
  }

  test("send a request correctly") {
    new HelperTrait {
      when(con.serve(any[ReqPacket])) thenReturn Future(RepPacket(None, None))
      preProcess.unlockService()
      preProcess(DeleteRequest("/zookeeper/test", -1))
      verify(con).serve(any[ReqPacket])
    }
  }

  test("send a request and get an exception") {
    new HelperTrait {
      when(con.serve(any[ReqPacket])) thenReturn Future.exception(new CancelledRequestException())
      preProcess.unlockService()
      preProcess(DeleteRequest("/zookeeper/test", -1))
      verify(con).serve(any[ReqPacket])
    }
  }

  test("send a request and wait until permit is available") {
    new HelperTrait {
      when(con.serve(any[ReqPacket])) thenReturn Future.exception(new CancelledRequestException())
      preProcess.lockService()
      preProcess(DeleteRequest("/zookeeper/test", -1))
      preProcess.unlockService()
      verify(con).serve(any[ReqPacket])
      verify(preProcess).unlockService()
    }
  }

  test("lock the service") {
    new HelperTrait {
      when(con.serve(any[ReqPacket])) thenReturn Future.exception(new CancelledRequestException())
      preProcess.lockService()
      preProcess(DeleteRequest("/zookeeper/test", -1))
      verify(con, times(0)).serve(any[ReqPacket])
      verify(preProcess).lockService()
    }
  }

  test("unlock the service") {
    new HelperTrait {
      when(con.serve(any[ReqPacket])) thenReturn Future.exception(new CancelledRequestException())
      preProcess.lockService()
      preProcess(DeleteRequest("/zookeeper/test", -1))
      preProcess(DeleteRequest("/zookeeper/test", -1))
      preProcess(DeleteRequest("/zookeeper/test", -1))
      verify(con, times(0)).serve(any[ReqPacket])
      verify(preProcess).lockService()
      preProcess.unlockService()
      verify(con, times(3)).serve(any[ReqPacket])
    }
  }

  test("should throw an exception will sending a write request in RO mode") {
    new HelperTrait {
      when(con.serve(any[ReqPacket])) thenReturn Future.exception(new CancelledRequestException())
      preProcess.unlockService()
      sessMngr.session.isRO.set(true)
      intercept[NotReadOnlyException] {
        Await.result(preProcess(CreateRequest("", "h".getBytes, Ids.OPEN_ACL_UNSAFE, 1)))
      }
      intercept[NotReadOnlyException] {
        Await.result(preProcess(Create2Request("", "h".getBytes, Ids.OPEN_ACL_UNSAFE, 1)))
      }
      intercept[NotReadOnlyException] {
        Await.result(preProcess(DeleteRequest("", 1)))
      }
      intercept[NotReadOnlyException] {
        Await.result(preProcess(ReconfigRequest("", "h", "", 1L)))
      }
      intercept[NotReadOnlyException] {
        Await.result(preProcess(SetACLRequest("", Ids.OPEN_ACL_UNSAFE, 1)))
      }
      intercept[NotReadOnlyException] {
        Await.result(preProcess(SetDataRequest("", "h".getBytes, 1)))
      }
      intercept[NotReadOnlyException] {
        Await.result(preProcess(SyncRequest("")))
      }
      intercept[NotReadOnlyException] {
        Await.result(preProcess(TransactionRequest(Seq.empty[OpRequest])))
      }
    }
  }
}