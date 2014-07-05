package com.twitter.finagle.exp.zookeeper.unit

import com.twitter.finagle.exp.zookeeper._
import com.twitter.finagle.exp.zookeeper.data.{Auth, Ids}
import com.twitter.finagle.exp.zookeeper.transport._
import com.twitter.finagle.exp.zookeeper.watch.Watch
import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.{CreateMode, OpCode}
import com.twitter.io.Buf
import com.twitter.util.TimeConversions._
import org.scalatest.FunSuite

class ReqPacketEncodingTest extends FunSuite {
  test("ConnectRequest encoding") {
    val connectReq = new ConnectRequest(
      0, 0L, 2000.milliseconds, 0L, Array[Byte](16), true)
    val packetReq = ReqPacket(None, Some(connectReq))

    val encoderBuf = Buf.Empty
      .concat(Buf.U32BE(0))
      .concat(Buf.U64BE(0L))
      .concat(Buf.U32BE(2000.milliseconds.inMilliseconds.toInt))
      .concat(Buf.U64BE(0L))
      .concat(BufArray(Array[Byte](16)))
      .concat(BufBool(true))

    assert(packetReq.buf === encoderBuf)
  }

  test("RequestHeader encoding") {
    val requestHeader = new RequestHeader(732, OpCode.PING)
    val packetReq = ReqPacket(Some(requestHeader), None)

    val encoderBuf = Buf.Empty
      .concat(Buf.U32BE(732))
      .concat(Buf.U32BE(OpCode.PING))

    assert(packetReq.buf === encoderBuf)
  }

  test("CreateRequest encoding") {
    val requestHeader = new RequestHeader(732, OpCode.CREATE)
    val createRequest = new CreateRequest(
      "/zookeeeper/test", "CHANGE".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
    val packetReq = ReqPacket(Some(requestHeader), Some(createRequest))

    val encoderBuf = Buf.Empty
      .concat(Buf.U32BE(732))
      .concat(Buf.U32BE(OpCode.CREATE))
      .concat(BufString("/zookeeeper/test"))
      .concat(BufArray("CHANGE".getBytes))
      .concat(BufSeqACL(Ids.OPEN_ACL_UNSAFE))
      .concat(Buf.U32BE(CreateMode.EPHEMERAL))

    assert(packetReq.buf === encoderBuf)
  }

  test("GetACLRequest encoding") {
    val requestHeader = new RequestHeader(732, OpCode.GET_ACL)
    val getAclRequest = new GetACLRequest("/zookeeper/test")
    val packetReq = ReqPacket(Some(requestHeader), Some(getAclRequest))

    val encoderBuf = Buf.Empty
      .concat(Buf.U32BE(732))
      .concat(Buf.U32BE(OpCode.GET_ACL))
      .concat(BufString("/zookeeper/test"))

    assert(packetReq.buf === encoderBuf)
  }

  test("GetData encoding") {
    val requestHeader = new RequestHeader(732, OpCode.GET_DATA)
    val getDataRequest = new GetDataRequest("/zookeeper/test", true)
    val packetReq = ReqPacket(Some(requestHeader), Some(getDataRequest))

    val encoderBuf = Buf.Empty
      .concat(Buf.U32BE(732))
      .concat(Buf.U32BE(OpCode.GET_DATA))
      .concat(BufString("/zookeeper/test"))
      .concat(BufBool(true))

    assert(packetReq.buf === encoderBuf)
  }

  test("DeleteRequest encoding") {
    val requestHeader = new RequestHeader(732, OpCode.DELETE)
    val deleteRequest = new DeleteRequest("/zookeeper/test", 1)
    val packetReq = ReqPacket(Some(requestHeader), Some(deleteRequest))

    val encoderBuf = Buf.Empty
      .concat(Buf.U32BE(732))
      .concat(Buf.U32BE(OpCode.DELETE))
      .concat(BufString("/zookeeper/test"))
      .concat(Buf.U32BE(1))

    assert(packetReq.buf === encoderBuf)
  }

  test("ExistsRequest encoding") {
    val requestHeader = new RequestHeader(732, OpCode.EXISTS)
    val existsRequest = new ExistsRequest("/zookeeper/test", true)
    val packetReq = ReqPacket(Some(requestHeader), Some(existsRequest))

    val encoderBuf = Buf.Empty
      .concat(Buf.U32BE(732))
      .concat(Buf.U32BE(OpCode.EXISTS))
      .concat(BufString("/zookeeper/test"))
      .concat(BufBool(true))

    assert(packetReq.buf === encoderBuf)
  }

  test("SetDataRequest encoding") {
    val requestHeader = new RequestHeader(732, OpCode.SET_DATA)
    val setDataRequest = new SetDataRequest(
      "/zookeeper/test", "CHANGE IT TO THIS".getBytes, 1)
    val packetReq = ReqPacket(Some(requestHeader), Some(setDataRequest))

    val encoderBuf = Buf.Empty
      .concat(Buf.U32BE(732))
      .concat(Buf.U32BE(OpCode.SET_DATA))
      .concat(BufString("/zookeeper/test"))
      .concat(BufArray("CHANGE IT TO THIS".getBytes))
      .concat(Buf.U32BE(1))

    assert(packetReq.buf === encoderBuf)
  }

  test("GetChildrenRequest encoding") {
    val requestHeader = new RequestHeader(732, OpCode.GET_CHILDREN)
    val getChildrenRequest = new GetChildrenRequest("/zookeeper/test", true)
    val packetReq = ReqPacket(Some(requestHeader), Some(getChildrenRequest))

    val encoderBuf = Buf.Empty
      .concat(Buf.U32BE(732))
      .concat(Buf.U32BE(OpCode.GET_CHILDREN))
      .concat(BufString("/zookeeper/test"))
      .concat(BufBool(true))

    assert(packetReq.buf === encoderBuf)
  }

  test("GetChildren2Request encoding") {
    val requestHeader = new RequestHeader(732, OpCode.GET_CHILDREN2)
    val getChildren2Request = new GetChildren2Request("/zookeeper/test", true)
    val packetReq = ReqPacket(Some(requestHeader), Some(getChildren2Request))

    val encoderBuf = Buf.Empty
      .concat(Buf.U32BE(732))
      .concat(Buf.U32BE(OpCode.GET_CHILDREN2))
      .concat(BufString("/zookeeper/test"))
      .concat(BufBool(true))

    assert(packetReq.buf === encoderBuf)
  }

  test("SetACLRequest encoding") {
    val requestHeader = new RequestHeader(732, OpCode.SET_ACL)
    val setACLRequest = new SetACLRequest("/zookeeper/test", Ids.OPEN_ACL_UNSAFE, 1)
    val packetReq = ReqPacket(Some(requestHeader), Some(setACLRequest))

    val encoderBuf = Buf.Empty
      .concat(Buf.U32BE(732))
      .concat(Buf.U32BE(OpCode.SET_ACL))
      .concat(BufString("/zookeeper/test"))
      .concat(BufSeqACL(Ids.OPEN_ACL_UNSAFE))
      .concat(Buf.U32BE(1))

    assert(packetReq.buf === encoderBuf)
  }

  test("SyncRequest encoding") {
    val requestHeader = new RequestHeader(732, OpCode.SYNC)
    val syncRequest = new SyncRequest("/zookeeper/test")
    val packetReq = ReqPacket(Some(requestHeader), Some(syncRequest))

    val encoderBuf = Buf.Empty
      .concat(Buf.U32BE(732))
      .concat(Buf.U32BE(OpCode.SYNC))
      .concat(BufString("/zookeeper/test"))

    assert(packetReq.buf === encoderBuf)
  }

  test("SetWatchesRequest encoding") {
    val requestHeader = new RequestHeader(732, OpCode.SET_WATCHES)
    val setWatchesRequest = new SetWatchesRequest(
      750L,
      Seq("/zookeeper/test", "/zookeeper/hello"),
      Seq("/zookeeper/test", "/zookeeper/hello"),
      Seq("/zookeeper/test", "/zookeeper/hello"))
    val packetReq = ReqPacket(Some(requestHeader), Some(setWatchesRequest))

    val encoderBuf = Buf.Empty
      .concat(Buf.U32BE(732))
      .concat(Buf.U32BE(OpCode.SET_WATCHES))
      .concat(Buf.U64BE(750L))
      .concat(BufSeqString(Seq("/zookeeper/test", "/zookeeper/hello")))
      .concat(BufSeqString(Seq("/zookeeper/test", "/zookeeper/hello")))
      .concat(BufSeqString(Seq("/zookeeper/test", "/zookeeper/hello")))

    assert(packetReq.buf === encoderBuf)
  }

  test("MultiHeader encoding") {
    val multiHeader = MultiHeader(OpCode.CHECK, false, -1)

    val encoderBuf = Buf.Empty
      .concat(Buf.U32BE(OpCode.CHECK))
      .concat(BufBool(false))
      .concat(Buf.U32BE(-1))

    assert(multiHeader.buf === encoderBuf)
  }

  test("Transaction encoding") {
    val requestHeader = new RequestHeader(732, OpCode.MULTI)
    val transactionRequest = new TransactionRequest(Seq(
      CreateRequest(
        "/zookeeper/test", "change".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL),
      Create2Request(
        "/zookeeper/test1", "change".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL),
      SetDataRequest("/zookeeper/test", "changed".getBytes, -1),
      CheckVersionRequest("/zookeeper/test", 2),
      DeleteRequest("/zookeeper/test", -1)
    ))
    val packetReq = ReqPacket(Some(requestHeader), Some(transactionRequest))

    val encoderBuf = Buf.Empty
      .concat(Buf.U32BE(732))
      .concat(Buf.U32BE(OpCode.MULTI))

      .concat(MultiHeader(OpCode.CREATE, false, -1).buf)
      .concat(BufString("/zookeeper/test"))
      .concat(BufArray("change".getBytes))
      .concat(BufSeqACL(Ids.OPEN_ACL_UNSAFE))
      .concat(Buf.U32BE(CreateMode.EPHEMERAL))

      .concat(MultiHeader(OpCode.CREATE2, false, -1).buf)
      .concat(BufString("/zookeeper/test1"))
      .concat(BufArray("change".getBytes))
      .concat(BufSeqACL(Ids.OPEN_ACL_UNSAFE))
      .concat(Buf.U32BE(CreateMode.EPHEMERAL))

      .concat(MultiHeader(OpCode.SET_DATA, false, -1).buf)
      .concat(BufString("/zookeeper/test"))
      .concat(BufArray("changed".getBytes))
      .concat(Buf.U32BE(-1))

      .concat(MultiHeader(OpCode.CHECK, false, -1).buf)
      .concat(BufString("/zookeeper/test"))
      .concat(Buf.U32BE(2))

      .concat(MultiHeader(OpCode.DELETE, false, -1).buf)
      .concat(BufString("/zookeeper/test"))
      .concat(Buf.U32BE(-1))

      .concat(MultiHeader(-1, true, -1).buf)

    assert(packetReq.buf === encoderBuf)
  }

  test("RemoveWatchesRequest") {
    val requestHeader = new RequestHeader(732, OpCode.REMOVE_WATCHES)
    val removeWatchesRequest = new RemoveWatchesRequest(
      "/zookeeper/test", Watch.Type.exists)
    val packetReq = ReqPacket(Some(requestHeader), Some(removeWatchesRequest))

    val encoderBuf = Buf.Empty
      .concat(Buf.U32BE(732))
      .concat(Buf.U32BE(OpCode.REMOVE_WATCHES))
      .concat(BufString("/zookeeper/test"))
      .concat(Buf.U32BE(Watch.Type.exists))

    assert(packetReq.buf === encoderBuf)
  }

  test("ReconfigRequest") {
    val requestHeader = new RequestHeader(732, OpCode.RECONFIG)
    val reconfigRequest = new ReconfigRequest(
      "192.168.10.10", "192.168.10.13", "192.168.10.20", 0L)
    val packetReq = ReqPacket(Some(requestHeader), Some(reconfigRequest))

    val encoderBuf = Buf.Empty
      .concat(Buf.U32BE(732))
      .concat(Buf.U32BE(OpCode.RECONFIG))
      .concat(BufString("192.168.10.10"))
      .concat(BufString("192.168.10.13"))
      .concat(BufString("192.168.10.20"))
      .concat(Buf.U64BE(0L))

    assert(packetReq.buf === encoderBuf)
  }

  test("AuthRequest") {
    val requestHeader = new RequestHeader(732, OpCode.AUTH)
    val authRequest = new AuthRequest(0, Auth("digest", "toto:nocall".getBytes))
    val packetReq = ReqPacket(Some(requestHeader), Some(authRequest))

    val encoderBuf = Buf.Empty
      .concat(Buf.U32BE(732))
      .concat(Buf.U32BE(OpCode.AUTH))
      .concat(Buf.U32BE(0))
      .concat(BufString("digest"))
      .concat(BufArray("toto:nocall".getBytes))

    assert(packetReq.buf === encoderBuf)
  }

  test("CheckWatchesRequest") {
    val requestHeader = new RequestHeader(732, OpCode.CHECK_WATCHES)
    val checkWatchesRequest = new CheckWatchesRequest("/zookeeper/test", Watch.Type.data)
    val packetReq = ReqPacket(Some(requestHeader), Some(checkWatchesRequest))

    val encoderBuf = Buf.Empty
      .concat(Buf.U32BE(732))
      .concat(Buf.U32BE(OpCode.CHECK_WATCHES))
      .concat(BufString("/zookeeper/test"))
      .concat(Buf.U32BE(Watch.Type.data))

    assert(packetReq.buf === encoderBuf)
  }
}