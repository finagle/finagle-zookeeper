package com.twitter.finagle.exp.zookeeper.unit.transport

import com.twitter.finagle.exp.zookeeper.{GetDataRequest, DeleteRequest}
import com.twitter.finagle.exp.zookeeper.transport.ZkTransport
import com.twitter.finagle.transport.Transport
import com.twitter.io.Buf
import com.twitter.util._
import java.io.IOException
import java.util
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito
import org.mockito.Mockito.{times, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class ZkTransportTest extends FunSuite with MockitoSugar {
  trait TestHelper {
    val deleteReq = DeleteRequest("/zookeeper/node", -1)
    val cbTransport = mock[Transport[ChannelBuffer, ChannelBuffer]]
    val zkTransport = Mockito.spy(new ZkTransport(cbTransport))
  }

  test("should convert Buf to ChannelBuffer") {
    new TestHelper {
      when(cbTransport.write(any[ChannelBuffer])) thenAnswer {
        new Answer[Future[Unit]] {
          override def answer(invocation: InvocationOnMock): Future[Unit] = {
            val cb = invocation.getArguments.head.asInstanceOf[ChannelBuffer]
            val bytes = new Array[Byte](deleteReq.buf.length + 4)
            Buf.Empty
              .concat(Buf.U32BE(deleteReq.buf.length))
              .concat(deleteReq.buf)
              .write(bytes, 0)
            if (util.Arrays.equals(bytes, cb.array())) Future.Done
            else Future.exception(new RuntimeException("bad conversion"))
          }
        }
      }

      Await.result(zkTransport.write(deleteReq.buf))
      Await.result(zkTransport.write(deleteReq.buf))
      verify(zkTransport, times(2)).write(any[Buf])
      verify(cbTransport, times(2)).write(any[ChannelBuffer])
    }
  }

  test("should convert one rep from ChannelBuffer to buf") {
    new TestHelper {
      when(cbTransport.read()) thenReturn {
        val bytes = new Array[Byte](deleteReq.buf.length + 4)
        Buf.Empty
          .concat(Buf.U32BE(deleteReq.buf.length))
          .concat(deleteReq.buf)
          .write(bytes, 0)
        Future(ChannelBuffers.wrappedBuffer(bytes))
      }

      val falseRep = DeleteRequest("/woot/cat", -1)
      val rep = Await.result(zkTransport.read())
      verify(cbTransport).read()
      verify(zkTransport).read()
      verify(zkTransport, times(3)).read(any[Int])
      assert(rep != falseRep.buf)
      assert(rep === deleteReq.buf)
    }
  }

  test("should not convert ChannelBuffer to buf") {
    new TestHelper {
      when(cbTransport.read()) thenReturn {
        val bytes = new Array[Byte](deleteReq.buf.length + 4)
        Buf.Empty
          .concat(deleteReq.buf)
          .write(bytes, 0)
        Future(ChannelBuffers.wrappedBuffer(bytes))
      }

      val rep = Await.result(zkTransport.read())
      verify(zkTransport).read()
      verify(zkTransport, times(3)).read(any[Int])
      verify(cbTransport).read()
      assert(rep != deleteReq.buf)
    }
  }

  test("should throw an IOException for Buf > 1 MB") {
    new TestHelper {
      when(cbTransport.read()) thenReturn {
        val bytes = new Array[Byte](4097 * 1024)
        Future(ChannelBuffers
          .wrappedBuffer(bytes map (_ => 1.toByte)))
      }

      intercept[IOException] {
        Await.result(zkTransport.read())
      }
      verify(zkTransport).read()
      verify(zkTransport, times(2)).read(any[Int])
      verify(cbTransport).read()
    }
  }

  test("should convert 2 rep from ChannelBuffer to buf") {
    new TestHelper {
      when(cbTransport.read()) thenReturn {
        val bytes = new Array[Byte]((deleteReq.buf.length + 4) * 2)
        Buf.Empty
          .concat(Buf.U32BE(deleteReq.buf.length))
          .concat(deleteReq.buf)
          .concat(Buf.U32BE(deleteReq.buf.length))
          .concat(deleteReq.buf)
          .write(bytes, 0)
        Future(ChannelBuffers.wrappedBuffer(bytes))
      }

      val falseRep = DeleteRequest("/woot/cat", -1)
      val rep = Await.result(zkTransport.read())
      verify(cbTransport).read()

      assert(rep != falseRep.buf)
      assert(rep === deleteReq.buf)
      assert(Await.result(zkTransport.read()) === deleteReq.buf)
      verify(cbTransport).read()
      verify(zkTransport, times(2)).read()
      verify(zkTransport, times(5)).read(any[Int])
    }
  }

  test("should convert one good and one bad rep") {
    new TestHelper {
      when(cbTransport.read()) thenReturn {
        val bytes = new Array[Byte](4097 * 1024)
        Future(ChannelBuffers
          .wrappedBuffer(bytes map (_ => 1.toByte)))
      } thenReturn {
        val bytes = new Array[Byte](deleteReq.buf.length + 4)
        Buf.Empty
          .concat(Buf.U32BE(deleteReq.buf.length))
          .concat(deleteReq.buf)
          .write(bytes, 0)
        Future(ChannelBuffers.wrappedBuffer(bytes))
      }

      intercept[IOException] {
        Await.result(zkTransport.read())
      }
      val rep = Await.result(zkTransport.read())
      verify(cbTransport, times(2)).read()
      verify(zkTransport, times(2)).read()
      verify(zkTransport, times(5)).read(any[Int])
      assert(rep === deleteReq.buf)
    }
  }

  test("should transform correctly few reads") {
    val getData = GetDataRequest("/zookeeper/worker1/job2", false)
    new TestHelper {
      when(cbTransport.read()) thenReturn {
        val bytes = new Array[Byte](getData.buf.length + 4)
        Buf.Empty
          .concat(Buf.U32BE(getData.buf.length))
          .concat(getData.buf)
          .write(bytes, 0)
        Future(ChannelBuffers.wrappedBuffer(bytes))
      } thenReturn {
        val bytes = new Array[Byte](deleteReq.buf.length + 4)
        Buf.Empty
          .concat(Buf.U32BE(deleteReq.buf.length))
          .concat(deleteReq.buf)
          .write(bytes, 0)
        Future(ChannelBuffers.wrappedBuffer(bytes))
      } thenReturn {
        val bytes = new Array[Byte](getData.buf.length + 4)
        Buf.Empty
          .concat(Buf.U32BE(getData.buf.length))
          .concat(getData.buf)
          .write(bytes, 0)
        Future(ChannelBuffers.wrappedBuffer(bytes))
      } thenReturn {
        val bytes = new Array[Byte](deleteReq.buf.length + 4)
        Buf.Empty
          .concat(Buf.U32BE(deleteReq.buf.length))
          .concat(deleteReq.buf)
          .write(bytes, 0)
        Future(ChannelBuffers.wrappedBuffer(bytes))
      }

      assert(Await.result(zkTransport.read()) === getData.buf)
      assert(Await.result(zkTransport.read()) === deleteReq.buf)
      assert(Await.result(zkTransport.read()) === getData.buf)
      assert(Await.result(zkTransport.read()) === deleteReq.buf)
      verify(cbTransport, times(4)).read()
      verify(zkTransport, times(4)).read()
      verify(zkTransport, times(12)).read(any[Int])
    }
  }
}