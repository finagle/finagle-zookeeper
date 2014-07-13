package com.twitter.finagle.exp.zookeeper.integration

import com.twitter.finagle.ChannelWriteException
import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.{CreateMode, OpCode}
import com.twitter.finagle.exp.zookeeper._
import com.twitter.finagle.exp.zookeeper.data.Ids
import com.twitter.io.Buf
import com.twitter.io.Buf.ByteArray
import com.twitter.util.{Await, Future}
import com.twitter.util.TimeConversions._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ConnectionTest extends FunSuite {
  /* Configure your server here */
  val ipAddress: String = "127.0.0.1"
  val port: Int = 2181
  val timeOut: Long = 1000

  // TODO Test ping, close, request without connection
  // TODO Test multiple connection
  test("Server is in rw mode") {
    val client = BufClient.newSimpleClient(ipAddress + ":" + port)
    val service = client.apply()
    val rep = service flatMap (_.apply(
      ByteArray("isro".getBytes)
    ))

    val res = Await.result(rep)
    val Buf.Utf8(str) = res.slice(0, res.length)
    assert(str === "rw")
    Await.ready(client.close())
  }

  test("Connection using BufClient") {
    try {
      val client = BufClient.newClient(ipAddress + ":" + port)
      val service = client.apply()
      def serve(buf: Buf): Future[Unit] = service flatMap (_.apply(buf).unit)

      val conReq = ReqPacket(
        None,
        Some(new ConnectRequest(
          0,
          0L,
          5000.milliseconds,
          0L,
          Array[Byte](16),
          canBeRO = true)))

      val closeReq = ReqPacket(Some(RequestHeader(1, OpCode.CLOSE_SESSION)), None)

      val rep = serve(Buf.U32BE(conReq.buf.length).concat(conReq.buf)) before {
        serve(Buf.U32BE(closeReq.buf.length).concat(closeReq.buf))
      }

      Await.result(rep)
      Await.result(client.close())
    }
    catch {
      case exc: ChannelWriteException =>
        throw exc
    }
  }

  test("Test with 2 servers unavailable") {
    val client = ZooKeeper.newRichClient(
      "127.0.0.1:2121,127.0.0.1:2151," + ipAddress + ":" + port)
    Await.result(client.connect())

    val res = for {
      _ <- client.create("/zookeeper/test", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
      exi <- client.exists("/zookeeper/test", watch = false)
      set <- client.setData("/zookeeper/test", "CHANGE IS GOOD1".getBytes, -1)
      get <- client.getData("/zookeeper/test", watch = false)
      sync <- client.sync("/zookeeper")
    } yield (exi, set, get, sync)

    val ret = Await.result(res)
    ret._1 match {
      case rep: ExistsResponse => assert(rep.stat.get.dataLength === "HELLO".getBytes.length)
      case _ => throw new RuntimeException("Test failed")
    }
    assert(ret._2.dataLength === "CHANGE IS GOOD1".getBytes.length)
    assert(ret._3.data === "CHANGE IS GOOD1".getBytes)
    assert(ret._4 === "/zookeeper")

    Await.ready(client.closeSession())
    Await.ready(client.closeService())
  }
}