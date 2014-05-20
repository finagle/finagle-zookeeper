package com.twitter.finagle.exp.zookeeper.unit

import org.scalatest.FunSuite
import com.twitter.finagle.exp.zookeeper._
import com.twitter.finagle.exp.zookeeper.transport.BufferReader
import com.twitter.finagle.exp.zookeeper.ZookeeperDefinitions.{createMode, opCode}
import com.twitter.finagle.exp.zookeeper.ConnectRequest
import scala.Some
import com.twitter.finagle.exp.zookeeper.RequestHeader

class RequestEncodingTest extends FunSuite {
/*
  test("ConnectRequest encoding") {
    val connectRequest = new ConnectRequest(0, 12L, 2000, 0L, Array.fill(16)(1.toByte), Some(true))

    val cb = connectRequest.toChannelBuffer
    cb.resetReaderIndex()
    val reader = BufferReader(cb)

    //Read packet size
    assert(reader.readInt === -1)
    assert(reader.readInt === 0)
    assert(reader.readLong === 12L)
    assert(reader.readInt === 2000)
    assert(reader.readLong === 0L)
    assert(reader.readBuffer === Array.fill(16)(1.toByte))
    assert(reader.readBool === true)
  }

  test("RequestHeader encoding") {
    val requestHeader = new RequestHeader(732, 2)

    val cb = requestHeader.toChannelBuffer
    cb.resetReaderIndex()
    val reader = BufferReader(cb)

    //Read packet size
    assert(reader.readInt === -1)
    assert(reader.readInt === 732)
    assert(reader.readInt === 2)
  }

  test("CreateRequest encoding") {
    val requestHeader = new RequestHeader(732, opCode.create)
    val createRequestBody = new CreateRequestBody("/zookeeeper/test", "CHANGE".getBytes, ACL.defaultACL, createMode.EPHEMERAL)
    val createRequest = new CreateRequest(requestHeader, createRequestBody)

    val cb = createRequest.toChannelBuffer
    cb.resetReaderIndex()
    val reader = BufferReader(cb)

    //Read packet size
    assert(reader.readInt === -1)
    assert(reader.readInt === 732)
    assert(reader.readInt === opCode.create)
    reader.readString === "/zookeeeper/test"
    assert(reader.readBuffer === "CHANGE".getBytes)
    assert(ACL.decodeArray(reader) === ACL.defaultACL)
    assert(reader.readInt === createMode.EPHEMERAL)
  }

  test("GetACLRequest encoding") {
    val requestHeader = new RequestHeader(732, opCode.getACL)
    val getAclRequestBody = new GetACLRequestBody("/zookeeper/test")
    val getAclRequest = new GetACLRequest(requestHeader, getAclRequestBody)

    val cb = getAclRequest.toChannelBuffer
    cb.resetReaderIndex()
    val reader = BufferReader(cb)

    //Read packet size
    assert(reader.readInt === -1)
    assert(reader.readInt === 732)
    assert(reader.readInt === opCode.getACL)
    assert(reader.readString === "/zookeeper/test")
  }

  test("GetData encoding") {
    val requestHeader = new RequestHeader(732, opCode.getData)
    val getDataRequestBody = new GetDataRequestBody("/zookeeper/test", true)
    val getDataRequest = new GetDataRequest(requestHeader, getDataRequestBody)

    val cb = getDataRequest.toChannelBuffer
    cb.resetReaderIndex()
    val reader = BufferReader(cb)

    //Read packet size
    assert(reader.readInt === -1)
    assert(reader.readInt === 732)
    assert(reader.readInt === opCode.getData)
    assert(reader.readString === "/zookeeper/test")
    assert(reader.readBool === true)
  }

  test("DeleteRequest encoding") {
    val requestHeader = new RequestHeader(732, opCode.delete)
    val deleteRequestBody = new DeleteRequestBody("/zookeeper/test", 1)
    val deleteRequest = new DeleteRequest(requestHeader, deleteRequestBody)

    val cb = deleteRequest.toChannelBuffer
    cb.resetReaderIndex()
    val reader = BufferReader(cb)

    //Read packet size
    assert(reader.readInt === -1)
    assert(reader.readInt === 732)
    assert(reader.readInt === opCode.delete)
    assert(reader.readString === "/zookeeper/test")
    assert(reader.readInt === 1)
  }

  test("ExistsRequest encoding") {
    val requestHeader = new RequestHeader(732, opCode.exists)
    val body = new ExistsRequestBody("/zookeeper/test", true)
    val request = new ExistsRequest(requestHeader, body)

    val cb = request.toChannelBuffer
    cb.resetReaderIndex()
    val reader = BufferReader(cb)

    //Read packet size
    assert(reader.readInt === -1)
    assert(reader.readInt === 732)
    assert(reader.readInt === opCode.exists)
    assert(reader.readString === "/zookeeper/test")
    assert(reader.readBool === true)
  }

  test("SetDataRequest encoding") {
    val header = new RequestHeader(732, opCode.setData)
    val body = new SetDataRequestBody("/zookeeper/test", "CHANGE IT TO THIS".getBytes, 1)
    val request = new SetDataRequest(header, body)

    val cb = request.toChannelBuffer
    cb.resetReaderIndex()
    val reader = BufferReader(cb)

    //Read packet size
    assert(reader.readInt === -1)
    assert(reader.readInt === 732)
    assert(reader.readInt === opCode.setData)
    assert(reader.readString === "/zookeeper/test")
    assert(reader.readBuffer === "CHANGE IT TO THIS".getBytes)
    assert(reader.readInt === 1)
  }

  test("GetChildrenRequest encoding") {
    val header = new RequestHeader(732, opCode.getChildren)
    val body = new GetChildrenRequestBody("/zookeeper/test", true)
    val request = new GetChildrenRequest(header, body)

    val cb = request.toChannelBuffer
    cb.resetReaderIndex()
    val reader = BufferReader(cb)

    //Read packet size
    assert(reader.readInt === -1)
    assert(reader.readInt === 732)
    assert(reader.readInt === opCode.getChildren)
    assert(reader.readString === "/zookeeper/test")
    assert(reader.readBool === true)
  }

  test("GetChildren2Request encoding") {
    val header = new RequestHeader(732, opCode.getChildren2)
    val body = new GetChildren2RequestBody("/zookeeper/test", true)
    val request = new GetChildren2Request(header, body)

    val cb = request.toChannelBuffer
    cb.resetReaderIndex()
    val reader = BufferReader(cb)

    //Read packet size
    assert(reader.readInt === -1)
    assert(reader.readInt === 732)
    assert(reader.readInt === opCode.getChildren2)
    assert(reader.readString === "/zookeeper/test")
    assert(reader.readBool === true)
  }

  test("SetACLRequest encoding") {
    val header = new RequestHeader(732, opCode.setACL)
    val body = new SetACLRequestBody("/zookeeper/test", ACL.defaultACL, 1)
    val request = new SetACLRequest(header, body)

    val cb = request.toChannelBuffer
    cb.resetReaderIndex()
    val reader = BufferReader(cb)

    //Read packet size
    assert(reader.readInt === -1)
    assert(reader.readInt === 732)
    assert(reader.readInt === opCode.setACL)
    assert(reader.readString === "/zookeeper/test")
    assert(ACL.decodeArray(reader) === ACL.defaultACL)
    assert(reader.readInt === 1)
  }

  test("SyncRequest encoding") {
    val header = new RequestHeader(732, opCode.sync)
    val body = new SyncRequestBody("/zookeeper/test")
    val request = new SyncRequest(header, body)

    val cb = request.toChannelBuffer
    cb.resetReaderIndex()
    val reader = BufferReader(cb)

    //Read packet size
    assert(reader.readInt === -1)
    assert(reader.readInt === 732)
    assert(reader.readInt === opCode.sync)
    assert(reader.readString === "/zookeeper/test")
  }

  test("SetWatchesRequest encoding") {
    val header = new RequestHeader(732, opCode.setWatches)
    val body = new SetWatchesRequestBody(750, Array("/zookeeper/test", "/zookeeper/hello"), new Array[String](0), new Array[String](0))
    val request = new SetWatchesRequest(header, body)

    val cb = request.toChannelBuffer
    cb.resetReaderIndex()
    val reader = BufferReader(cb)

    //Read packet size
    assert(reader.readInt === -1)
    assert(reader.readInt === 732)
    assert(reader.readInt === opCode.setWatches)
    assert(reader.readInt === 750)
    assert(reader.readInt === 2)
    assert(reader.readString === "/zookeeper/test")
    assert(reader.readString === "/zookeeper/hello")
  }*/
}
