package com.twitter.finagle.exp.zookeeper.unit

import java.util

import com.twitter.finagle.exp.zookeeper._
import com.twitter.finagle.exp.zookeeper.data.{Stat, Ids}
import com.twitter.finagle.exp.zookeeper.transport._
import com.twitter.finagle.exp.zookeeper.watch.Watch
import com.twitter.io.Buf
import com.twitter.util.TimeConversions._
import org.scalatest.FunSuite

class ResponseDecodingTest extends FunSuite {
  test("Decode a ReplyHeader") {
    val replyHeader = ReplyHeader(1, 1L, 1)
    val readBuffer = Buf.Empty
      .concat(BufInt(1))
      .concat(BufLong(1L))
      .concat(BufInt(1))

    val (decodedRep, _) = ReplyHeader
      .unapply(readBuffer)
      .getOrElse(throw new RuntimeException)

    assert(decodedRep === replyHeader)
  }

  test("Decode a connect response") {
    val connectResponse = ConnectResponse(
      0, 3000.milliseconds, 123415646L, "password".getBytes, true)

    val readBuffer = Buf.Empty
      .concat(BufInt(0))
      .concat(BufInt(3000))
      .concat(BufLong(123415646L))
      .concat(BufArray("password".getBytes))
      .concat(BufBool(true))

    val (decodedRep, _) = ConnectResponse
      .unapply(readBuffer)
      .getOrElse(throw new RuntimeException)

    assert(connectResponse.isRO == decodedRep.isRO)
    assert(connectResponse.protocolVersion == decodedRep.protocolVersion)
    assert(connectResponse.sessionId == decodedRep.sessionId)
    assert(connectResponse.timeOut == decodedRep.timeOut)
    assert(util.Arrays.equals(connectResponse.passwd, decodedRep.passwd))
  }

  test("Decode a create response") {
    val createResponse = CreateResponse("/zookeeper/test")

    val readBuffer = Buf.Empty
      .concat(BufString("/zookeeper/test"))

    val (decodedRep, _) = CreateResponse
      .unapply(readBuffer)
      .getOrElse(throw new RuntimeException)

    assert(decodedRep === createResponse)
  }

  test("Decode a watch event") {
    val watchEvent = WatchEvent(
      Watch.EventType.NODE_CREATED, Watch.State.SYNC_CONNECTED, "/zookeeper/test")

    val readBuffer = Buf.Empty
      .concat(BufInt(Watch.EventType.NODE_CREATED))
      .concat(BufInt(Watch.State.SYNC_CONNECTED))
      .concat(BufString("/zookeeper/test"))

    val (decodedRep, _) = WatchEvent
      .unapply(readBuffer)
      .getOrElse(throw new RuntimeException)

    assert(decodedRep === watchEvent)
  }


  test("Decode an exists response") {
    val existsRep = ExistsResponse(Some(
      Stat(0L, 0L, 0L, 0L, 1, 1, 1, 0L, 1, 1, 0L)),
      None)

    val readBuffer = Buf.Empty
      .concat(BufLong(0L))
      .concat(BufLong(0L))
      .concat(BufLong(0L))
      .concat(BufLong(0L))
      .concat(BufInt(1))
      .concat(BufInt(1))
      .concat(BufInt(1))
      .concat(BufLong(0L))
      .concat(BufInt(1))
      .concat(BufInt(1))
      .concat(BufLong(0L))

    val (decodedRep, _) = ExistsResponse
      .unapply(readBuffer)
      .getOrElse(throw new RuntimeException)

    assert(decodedRep === existsRep)
  }

  test("Decode a getAcl response") {
    val getAclRep = GetACLResponse(Ids.OPEN_ACL_UNSAFE, Stat(
      0L, 0L, 0L, 0L, 1, 1, 1, 0L, 1, 1, 0L))

    val readBuffer = Buf.Empty
      .concat(BufSeqACL(Ids.OPEN_ACL_UNSAFE))
      .concat(BufLong(0L))
      .concat(BufLong(0L))
      .concat(BufLong(0L))
      .concat(BufLong(0L))
      .concat(BufInt(1))
      .concat(BufInt(1))
      .concat(BufInt(1))
      .concat(BufLong(0L))
      .concat(BufInt(1))
      .concat(BufInt(1))
      .concat(BufLong(0L))

    val (decodedRep, _) = GetACLResponse
      .unapply(readBuffer)
      .getOrElse(throw new RuntimeException)

    assert(decodedRep === getAclRep)
  }

  test("Decode a getChildren response") {
    val getChildrenRep = GetChildrenResponse(
      Seq("/zookeeper/test", "zookeeper/hello"), None)

    val readBuffer = Buf.Empty
      .concat(BufSeqString(Seq("/zookeeper/test", "zookeeper/hello")))

    val (decodedRep, _) = GetChildrenResponse
      .unapply(readBuffer)
      .getOrElse(throw new RuntimeException)

    assert(decodedRep === getChildrenRep)
  }

  test("Decode a getChildren2 response") {
    val getChildren2Rep = GetChildren2Response(
      Seq("/zookeeper/test", "zookeeper/hello"),
      Stat(0L, 0L, 0L, 0L, 1, 1, 1, 0L, 1, 1, 0L),
      None)

    val readBuffer = Buf.Empty
      .concat(BufSeqString(Seq("/zookeeper/test", "zookeeper/hello")))
      .concat(BufLong(0L))
      .concat(BufLong(0L))
      .concat(BufLong(0L))
      .concat(BufLong(0L))
      .concat(BufInt(1))
      .concat(BufInt(1))
      .concat(BufInt(1))
      .concat(BufLong(0L))
      .concat(BufInt(1))
      .concat(BufInt(1))
      .concat(BufLong(0L))

    val (decodedRep, _) = GetChildren2Response
      .unapply(readBuffer)
      .getOrElse(throw new RuntimeException)

    assert(decodedRep === getChildren2Rep)
  }

  test("Decode a getData response") {
    val getDataRep = GetDataResponse(
      "change".getBytes,
      Stat(0L, 0L, 0L, 0L, 1, 1, 1, 0L, 1, 1, 0L),
      None)

    val readBuffer = Buf.Empty
      .concat(BufArray("change".getBytes))
      .concat(BufLong(0L))
      .concat(BufLong(0L))
      .concat(BufLong(0L))
      .concat(BufLong(0L))
      .concat(BufInt(1))
      .concat(BufInt(1))
      .concat(BufInt(1))
      .concat(BufLong(0L))
      .concat(BufInt(1))
      .concat(BufInt(1))
      .concat(BufLong(0L))

    val (decodedRep, _) = GetDataResponse
      .unapply(readBuffer)
      .getOrElse(throw new RuntimeException)

    assert(util.Arrays.equals(decodedRep.data, getDataRep.data))
    assert(decodedRep.stat === getDataRep.stat)
  }

  test("Decode a setAcl response") {
    val setAclResponse = SetACLResponse(
      Stat(0L, 0L, 0L, 0L, 1, 1, 1, 0L, 1, 1, 0L))

    val readBuffer = Buf.Empty
      .concat(BufLong(0L))
      .concat(BufLong(0L))
      .concat(BufLong(0L))
      .concat(BufLong(0L))
      .concat(BufInt(1))
      .concat(BufInt(1))
      .concat(BufInt(1))
      .concat(BufLong(0L))
      .concat(BufInt(1))
      .concat(BufInt(1))
      .concat(BufLong(0L))

    val (decodedRep, _) = SetACLResponse
      .unapply(readBuffer)
      .getOrElse(throw new RuntimeException)

    assert(decodedRep === setAclResponse)
  }

  test("Decode a setData response") {
    val setDataRep = SetDataResponse(
      Stat(0L, 0L, 0L, 0L, 1, 1, 1, 0L, 1, 1, 0L))

    val readBuffer = Buf.Empty
      .concat(BufLong(0L))
      .concat(BufLong(0L))
      .concat(BufLong(0L))
      .concat(BufLong(0L))
      .concat(BufInt(1))
      .concat(BufInt(1))
      .concat(BufInt(1))
      .concat(BufLong(0L))
      .concat(BufInt(1))
      .concat(BufInt(1))
      .concat(BufLong(0L))

    val (decodedRep, _) = SetDataResponse
      .unapply(readBuffer)
      .getOrElse(throw new RuntimeException)

    assert(decodedRep === setDataRep)
  }
}