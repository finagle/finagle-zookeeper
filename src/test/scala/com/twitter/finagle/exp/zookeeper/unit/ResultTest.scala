package com.twitter.finagle.exp.zookeeper.unit

import org.scalatest.FunSuite
import com.twitter.finagle.exp.zookeeper.transport.{BufferWriter, Buffer}
import com.twitter.util.{Throw, Return, Try}
import com.twitter.finagle.exp.zookeeper._
import com.twitter.finagle.exp.zookeeper.ZookeeperDefinitions.opCode
import org.jboss.netty.buffer.ChannelBuffers._
import scala.Some

class ResultTest extends FunSuite {

  class Helper {
    val buffer = Buffer.getDynamicBuffer(16)
    val writer = BufferWriter(buffer)
  }

  test("Decode a connect response") {
    val h = new Helper
    import h._

    writer.write(-1)
    //Protocol version
    writer.write(0)
    //Negotiated time out
    writer.write(2000)
    //Session id
    writer.write(1L)
    //Password
    writer.write(Array[Byte](16))
    //Can be read only (not supported by every server version)
    writer.write(Some(true))

    //Write the packet size at index 0 and rewind
    val bb = writer.underlying.toByteBuffer
    bb.putInt(bb.capacity() - 4)
    bb.rewind()

    //This is the type of response received at decoding
    val rep = BufferedResponse(Buffer.fromChannelBuffer(wrappedBuffer(bb)))
    //Decode the raw response of connect response
    val pureRep: Try[ConnectResponse] = ResponseWrapper.decode(rep, opCode.createSession).asInstanceOf[Try[ConnectResponse]]

    assert(pureRep match {
      case Return(res) =>
        assert(res.protocolVersion === 0)
        assert(res.timeOut === 2000)
        assert(res.sessionId === 1L)
        assert(res.passwd === Array[Byte](16))
        assert(res.canRO === Some(true))
        true
    })
  }

  test("Decode a create response") {
    val h = new Helper
    import h._

    //ReplyHeader
    writer.write(-1)
    //Header
    //XID
    writer.write(745)
    //Zxid
    writer.write(1L)
    //Err
    writer.write(0)

    //Body
    //Path
    writer.write("/zookeeper/test")

    //Write the packet size at index 0 and rewind
    val bb = writer.underlying.toByteBuffer
    bb.putInt(bb.capacity() - 4)
    bb.rewind()

    //This is the type of response received at decoding
    val rep = BufferedResponse(Buffer.fromChannelBuffer(wrappedBuffer(bb)))
    val pureRep: Try[CreateResponse] = ResponseWrapper.decode(rep, opCode.create).asInstanceOf[Try[CreateResponse]]

    assert(pureRep match {
      case Return(res) =>
        assert(res.header.xid === 745)
        assert(res.header.zxid === 1L)
        assert(res.header.err === 0)
        assert(res.body.get.path === "/zookeeper/test")
        true
    })
  }

  test("Decode a delete response") {
    val h = new Helper
    import h._

    //ReplyHeader
    writer.write(-1)
    //XID
    writer.write(745)
    //ZXID
    writer.write(152L)
    //Err
    writer.write(0)

    //Write the packet size at index 0 and rewind
    val bb = writer.underlying.toByteBuffer
    bb.putInt(bb.capacity() - 4)
    bb.rewind()

    //This is the type of response received at decoding
    val rep = BufferedResponse(Buffer.fromChannelBuffer(wrappedBuffer(bb)))
    val pureRep = ResponseWrapper.decode(rep, opCode.delete).asInstanceOf[Try[ReplyHeader]]

    assert(pureRep match {
      case Return(res) =>
        assert(res.err === 0)
        assert(res.xid === 745)
        assert(res.zxid === 152)
        true
    })
  }

  test("Decode an error response (from a delete response)") {
    val h = new Helper
    import h._

    //ReplyHeader
    writer.write(-1)
    //XID
    writer.write(745)
    //ZXID
    writer.write(152L)
    //Err
    writer.write(-101)

    //Write the packet size at index 0 and rewind
    val bb = writer.underlying.toByteBuffer
    bb.putInt(bb.capacity() - 4)
    bb.rewind()

    //This is the type of response received at decoding
    val rep = BufferedResponse(Buffer.fromChannelBuffer(wrappedBuffer(bb)))

    val pureRep = ResponseWrapper.decode(rep, opCode.delete).asInstanceOf[Try[ReplyHeader]]

    assert(pureRep match {
      case Throw(ex) =>
        assert(ex.isInstanceOf[ZookeeperException])
        true
    })
  }



  test("Decode a watch event") {
    val h = new Helper

    import h._

    //ReplyHeader
    writer.write(-1)
    //XID
    writer.write(745)
    //ZXID
    writer.write(152L)
    //Err
    writer.write(0)

    //Body
    //Type
    writer.write(1)
    //state
    writer.write(3)
    //Path
    writer.write("/zookeeper/test")

    //Write the packet size at index 0 and rewind
    val bb = writer.underlying.toByteBuffer
    bb.putInt(bb.capacity() - 4)
    bb.rewind()

    //This is the type of response received at decoding
    val event = WatcherEvent.decode(Buffer.fromChannelBuffer(wrappedBuffer(bb)))
    assert(event.header.xid === 745)
    assert(event.header.zxid === 152)
    assert(event.header.err === 0)
    assert(event.body.get.typ === 1)
    assert(event.body.get.state === 3)
    assert(event.body.get.path === "/zookeeper/test")
  }

  test("Decode a disconnect response") {
    val h = new Helper
    import h._

    //ReplyHeader
    writer.write(-1)
    //XID
    writer.write(745)
    //ZXID
    writer.write(152L)
    //Err
    writer.write(0)

    //Write the packet size at index 0 and rewind
    val bb = writer.underlying.toByteBuffer
    bb.putInt(bb.capacity() - 4)
    bb.rewind()

    //This is the type of response received at decoding
    val rep = BufferedResponse(Buffer.fromChannelBuffer(wrappedBuffer(bb)))
    val pureRep = ResponseWrapper.decode(rep, opCode.closeSession).asInstanceOf[Try[ReplyHeader]]

    assert(pureRep match {
      case Return(res) =>
        assert(res.err === 0)
        assert(res.xid === 745)
        assert(res.zxid === 152)
        true
    })
  }

  test("Decode a ping response") {
    val h = new Helper
    import h._

    //ReplyHeader
    writer.write(-1)
    //XID
    writer.write(745)
    //ZXID
    writer.write(152L)
    //Err
    writer.write(0)

    //Write the packet size at index 0 and rewind
    val bb = writer.underlying.toByteBuffer
    bb.putInt(bb.capacity() - 4)
    bb.rewind()

    //This is the type of response received at decoding
    val rep = BufferedResponse(Buffer.fromChannelBuffer(wrappedBuffer(bb)))
    val pureRep = ResponseWrapper.decode(rep, opCode.ping).asInstanceOf[Try[ReplyHeader]]

    assert(pureRep match {
      case Return(res) =>
        assert(res.err === 0)
        assert(res.xid === 745)
        assert(res.zxid === 152)
        true
    })
  }

  test("Decode an exists response") {
    val h = new Helper
    import h._

    //ReplyHeader
    writer.write(-1)
    //XID
    writer.write(745)
    //ZXID
    writer.write(152L)
    //Err
    writer.write(0)

    //Body
    //Stat
    //czxid
    writer.write(1L)
    //mzxid
    writer.write(2L)
    //ctime
    writer.write(3L)
    //mtime
    writer.write(4L)
    //version
    writer.write(1)
    //cversion
    writer.write(2)
    //aversion
    writer.write(3)
    //ephemeralOwner
    writer.write(5L)
    //dataLength
    writer.write(1455689)
    //numChildren
    writer.write(0)
    //pzxid
    writer.write(1225L)

    //Write the packet size at index 0 and rewind
    val bb = writer.underlying.toByteBuffer
    bb.putInt(bb.capacity() - 4)
    bb.rewind()

    //This is the type of response received at decoding
    val rep = BufferedResponse(Buffer.fromChannelBuffer(wrappedBuffer(bb)))
    val pureRep = ResponseWrapper.decode(rep, opCode.exists).asInstanceOf[Try[ExistsResponse]]

    assert(pureRep match {
      case Return(res) =>
        assert(res.header.xid === 745)
        assert(res.header.zxid === 152L)
        assert(res.header.err === 0)
        assert(res.body.get.stat.czxid === 1L)
        assert(res.body.get.stat.mzxid === 2L)
        assert(res.body.get.stat.ctime === 3L)
        assert(res.body.get.stat.mtime === 4L)
        assert(res.body.get.stat.version === 1)
        assert(res.body.get.stat.cversion === 2)
        assert(res.body.get.stat.aversion === 3)
        assert(res.body.get.stat.ephemeralOwner === 5L)
        assert(res.body.get.stat.dataLength === 1455689)
        assert(res.body.get.stat.numChildren === 0)
        assert(res.body.get.stat.pzxid === 1225L)
        true
    })
  }

  test("Decode a getAcl response") {
    val h = new Helper
    import h._

    //ReplyHeader
    writer.write(-1)
    //XID
    writer.write(745)
    //ZXID
    writer.write(152L)
    //Err
    writer.write(0)

    //Body
    //ACL
    val aclList = Array.fill(1)(new ACL(31, new ID("world", "anyone")))
    writer.write(aclList)

    //Stat
    //czxid
    writer.write(1L)
    //mzxid
    writer.write(2L)
    //ctime
    writer.write(3L)
    //mtime
    writer.write(4L)
    //version
    writer.write(1)
    //cversion
    writer.write(2)
    //aversion
    writer.write(3)
    //ephemeralOwner
    writer.write(5L)
    //dataLength
    writer.write(1455689)
    //numChildren
    writer.write(0)
    //pzxid
    writer.write(1225L)


    //Write the packet size at index 0 and rewind
    val bb = writer.underlying.toByteBuffer
    bb.putInt(bb.capacity() - 4)
    bb.rewind()

    //This is the type of response received at decoding
    val rep = BufferedResponse(Buffer.fromChannelBuffer(wrappedBuffer(bb)))
    val pureRep = ResponseWrapper.decode(rep, opCode.getACL).asInstanceOf[Try[GetACLResponse]]

    assert(pureRep match {
      case Return(res) =>
        assert(res.header.xid === 745)
        assert(res.header.zxid === 152L)
        assert(res.header.err === 0)
        assert(res.body.get.acl(0).perms === 31)
        assert(res.body.get.acl(0).id.id === "anyone")
        assert(res.body.get.acl(0).id.scheme === "world")
        assert(res.body.get.stat.czxid === 1L)
        assert(res.body.get.stat.mzxid === 2L)
        assert(res.body.get.stat.ctime === 3L)
        assert(res.body.get.stat.mtime === 4L)
        assert(res.body.get.stat.version === 1)
        assert(res.body.get.stat.cversion === 2)
        assert(res.body.get.stat.aversion === 3)
        assert(res.body.get.stat.ephemeralOwner === 5L)
        assert(res.body.get.stat.dataLength === 1455689)
        assert(res.body.get.stat.numChildren === 0)
        assert(res.body.get.stat.pzxid === 1225L)
        true
    })
  }

  test("Decode a getChildren response") {
    val h = new Helper
    import h._

    //ReplyHeader
    writer.write(-1)
    //XID
    writer.write(745)
    //ZXID
    writer.write(152L)
    //Err
    writer.write(0)

    //Body
    //Children
    val children = Array[String]("/zookeeper/test", "/zookeeper/hello", "zookeeper/real")
    writer.write(children)

    //Write the packet size at index 0 and rewind
    val bb = writer.underlying.toByteBuffer
    bb.putInt(bb.capacity() - 4)
    bb.rewind()

    //This is the type of response received at decoding
    val rep = BufferedResponse(Buffer.fromChannelBuffer(wrappedBuffer(bb)))
    val pureRep = ResponseWrapper.decode(rep, opCode.getChildren).asInstanceOf[Try[GetChildrenResponse]]

    assert(pureRep match {
      case Return(res) =>
        assert(res.header.xid === 745)
        assert(res.header.zxid === 152L)
        assert(res.header.err === 0)
        assert(res.body.get.children(0) === children(0))
        assert(res.body.get.children(1) === children(1))
        assert(res.body.get.children(2) === children(2))
        true
    })
  }

  test("Decode a getChildren2 response") {
    val h = new Helper
    import h._

    //ReplyHeader
    writer.write(-1)
    //XID
    writer.write(745)
    //ZXID
    writer.write(152L)
    //Err
    writer.write(0)

    //Body
    //Children
    val children = Array[String]("/zookeeper/test", "/zookeeper/hello", "zookeeper/real")
    writer.write(children)

    //Stat
    //czxid
    writer.write(1L)
    //mzxid
    writer.write(2L)
    //ctime
    writer.write(3L)
    //mtime
    writer.write(4L)
    //version
    writer.write(1)
    //cversion
    writer.write(2)
    //aversion
    writer.write(3)
    //ephemeralOwner
    writer.write(5L)
    //dataLength
    writer.write(1455689)
    //numChildren
    writer.write(0)
    //pzxid
    writer.write(1225L)


    //Write the packet size at index 0 and rewind
    val bb = writer.underlying.toByteBuffer
    bb.putInt(bb.capacity() - 4)
    bb.rewind()

    //This is the type of response received at decoding
    val rep = BufferedResponse(Buffer.fromChannelBuffer(wrappedBuffer(bb)))
    val pureRep = ResponseWrapper.decode(rep, opCode.getChildren2).asInstanceOf[Try[GetChildren2Response]]

    assert(pureRep match {
      case Return(res) =>
        assert(res.header.xid === 745)
        assert(res.header.zxid === 152L)
        assert(res.header.err === 0)
        assert(res.body.get.children(0) === children(0))
        assert(res.body.get.children(1) === children(1))
        assert(res.body.get.children(2) === children(2))
        assert(res.body.get.stat.czxid === 1L)
        assert(res.body.get.stat.mzxid === 2L)
        assert(res.body.get.stat.ctime === 3L)
        assert(res.body.get.stat.mtime === 4L)
        assert(res.body.get.stat.version === 1)
        assert(res.body.get.stat.cversion === 2)
        assert(res.body.get.stat.aversion === 3)
        assert(res.body.get.stat.ephemeralOwner === 5L)
        assert(res.body.get.stat.dataLength === 1455689)
        assert(res.body.get.stat.numChildren === 0)
        assert(res.body.get.stat.pzxid === 1225L)
        true
    })
  }

  test("Decode a getData response") {
    val h = new Helper
    import h._

    //ReplyHeader
    writer.write(-1)
    //XID
    writer.write(745)
    //ZXID
    writer.write(152L)
    //Err
    writer.write(0)

    //Body
    //Data
    writer.write(Array[Byte](1561e10.toByte))
    //Stat
    //czxid
    writer.write(1L)
    //mzxid
    writer.write(2L)
    //ctime
    writer.write(3L)
    //mtime
    writer.write(4L)
    //version
    writer.write(1)
    //cversion
    writer.write(2)
    //aversion
    writer.write(3)
    //ephemeralOwner
    writer.write(5L)
    //dataLength
    writer.write(1455689)
    //numChildren
    writer.write(0)
    //pzxid
    writer.write(1225L)

    //Write the packet size at index 0 and rewind
    val bb = writer.underlying.toByteBuffer
    bb.putInt(bb.capacity() - 4)
    bb.rewind()

    //This is the type of response received at decoding
    val rep = BufferedResponse(Buffer.fromChannelBuffer(wrappedBuffer(bb)))
    val pureRep = ResponseWrapper.decode(rep, opCode.getData).asInstanceOf[Try[GetDataResponse]]

    assert(pureRep match {
      case Return(res) =>
        assert(res.header.xid === 745)
        assert(res.header.zxid === 152L)
        assert(res.header.err === 0)
        assert(res.body.get.data === Array[Byte](1561e10.toByte))
        assert(res.body.get.stat.czxid === 1L)
        assert(res.body.get.stat.mzxid === 2L)
        assert(res.body.get.stat.ctime === 3L)
        assert(res.body.get.stat.mtime === 4L)
        assert(res.body.get.stat.version === 1)
        assert(res.body.get.stat.cversion === 2)
        assert(res.body.get.stat.aversion === 3)
        assert(res.body.get.stat.ephemeralOwner === 5L)
        assert(res.body.get.stat.dataLength === 1455689)
        assert(res.body.get.stat.numChildren === 0)
        assert(res.body.get.stat.pzxid === 1225L)
        true
    })
  }

  test("Decode a setAcl response") {
    val h = new Helper
    import h._

    //ReplyHeader
    writer.write(-1)
    //XID
    writer.write(745)
    //ZXID
    writer.write(152L)
    //Err
    writer.write(0)

    //Body
    //Stat
    //czxid
    writer.write(1L)
    //mzxid
    writer.write(2L)
    //ctime
    writer.write(3L)
    //mtime
    writer.write(4L)
    //version
    writer.write(1)
    //cversion
    writer.write(2)
    //aversion
    writer.write(3)
    //ephemeralOwner
    writer.write(5L)
    //dataLength
    writer.write(1455689)
    //numChildren
    writer.write(0)
    //pzxid
    writer.write(1225L)

    //Write the packet size at index 0 and rewind
    val bb = writer.underlying.toByteBuffer
    bb.putInt(bb.capacity() - 4)
    bb.rewind()

    //This is the type of response received at decoding
    val rep = BufferedResponse(Buffer.fromChannelBuffer(wrappedBuffer(bb)))
    val pureRep = ResponseWrapper.decode(rep, opCode.setACL).asInstanceOf[Try[SetACLResponse]]

    assert(pureRep match {
      case Return(res) =>
        assert(res.header.xid === 745)
        assert(res.header.zxid === 152L)
        assert(res.header.err === 0)
        assert(res.body.get.stat.czxid === 1L)
        assert(res.body.get.stat.mzxid === 2L)
        assert(res.body.get.stat.ctime === 3L)
        assert(res.body.get.stat.mtime === 4L)
        assert(res.body.get.stat.version === 1)
        assert(res.body.get.stat.cversion === 2)
        assert(res.body.get.stat.aversion === 3)
        assert(res.body.get.stat.ephemeralOwner === 5L)
        assert(res.body.get.stat.dataLength === 1455689)
        assert(res.body.get.stat.numChildren === 0)
        assert(res.body.get.stat.pzxid === 1225L)
        true
    })
  }

  test("Decode a setData response") {
    val h = new Helper
    import h._

    //ReplyHeader
    writer.write(-1)
    //XID
    writer.write(745)
    //ZXID
    writer.write(152L)
    //Err
    writer.write(0)

    //Body
    //Stat
    //czxid
    writer.write(1L)
    //mzxid
    writer.write(2L)
    //ctime
    writer.write(3L)
    //mtime
    writer.write(4L)
    //version
    writer.write(1)
    //cversion
    writer.write(2)
    //aversion
    writer.write(3)
    //ephemeralOwner
    writer.write(5L)
    //dataLength
    writer.write(1455689)
    //numChildren
    writer.write(0)
    //pzxid
    writer.write(1225L)

    //Write the packet size at index 0 and rewind
    val bb = writer.underlying.toByteBuffer
    bb.putInt(bb.capacity() - 4)
    bb.rewind()

    //This is the type of response received at decoding
    val rep = BufferedResponse(Buffer.fromChannelBuffer(wrappedBuffer(bb)))
    val pureRep = ResponseWrapper.decode(rep, opCode.setData).asInstanceOf[Try[SetDataResponse]]

    assert(pureRep match {
      case Return(res) =>
        assert(res.header.xid === 745)
        assert(res.header.zxid === 152L)
        assert(res.header.err === 0)
        assert(res.body.get.stat.czxid === 1L)
        assert(res.body.get.stat.mzxid === 2L)
        assert(res.body.get.stat.ctime === 3L)
        assert(res.body.get.stat.mtime === 4L)
        assert(res.body.get.stat.version === 1)
        assert(res.body.get.stat.cversion === 2)
        assert(res.body.get.stat.aversion === 3)
        assert(res.body.get.stat.ephemeralOwner === 5L)
        assert(res.body.get.stat.dataLength === 1455689)
        assert(res.body.get.stat.numChildren === 0)
        assert(res.body.get.stat.pzxid === 1225L)
        true
    })
  }

  test("Decode a setWatches response") {
    val h = new Helper
    import h._

    //ReplyHeader
    writer.write(-1)
    //XID
    writer.write(745)
    //ZXID
    writer.write(152L)
    //Err
    writer.write(0)


    //Write the packet size at index 0 and rewind
    val bb = writer.underlying.toByteBuffer
    bb.putInt(bb.capacity() - 4)
    bb.rewind()

    //This is the type of response received at decoding
    val rep = BufferedResponse(Buffer.fromChannelBuffer(wrappedBuffer(bb)))
    val pureRep = ResponseWrapper.decode(rep, opCode.setWatches).asInstanceOf[Try[ReplyHeader]]

    assert(pureRep match {
      case Return(res) =>
        assert(res.xid === 745)
        assert(res.zxid === 152L)
        assert(res.err === 0)
        true
    })
  }

  test("Decode a sync response") {
    val h = new Helper
    import h._

    //ReplyHeader
    writer.write(-1)
    //XID
    writer.write(745)
    //ZXID
    writer.write(152L)
    //Err
    writer.write(0)

    //Body
    //Path
    writer.write("/zookeeper/test/real/world/case/here/for/real")

    //Write the packet size at index 0 and rewind
    val bb = writer.underlying.toByteBuffer
    bb.putInt(bb.capacity() - 4)
    bb.rewind()

    //This is the type of response received at decoding
    val rep = BufferedResponse(Buffer.fromChannelBuffer(wrappedBuffer(bb)))
    val pureRep = ResponseWrapper.decode(rep, opCode.sync).asInstanceOf[Try[SyncResponse]]

    assert(pureRep match {
      case Return(res) =>
        assert(res.header.xid === 745)
        assert(res.header.zxid === 152L)
        assert(res.header.err === 0)
        assert(res.body.get.path === "/zookeeper/test/real/world/case/here/for/real")
        true
    })
  }
}
