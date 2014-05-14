package com.twitter.finagle.exp.zookeeper

import com.twitter.finagle.exp.zookeeper.transport.{Buffer, BufferWriter, BufferReader}
import com.twitter.util.Try

sealed trait Data

sealed trait serializable {
  def toBuffer: Buffer
}

trait DecoderData[T <: Data] extends (BufferReader => Try[T]) {
  def apply(buffer: BufferReader): Try[T] = Try(decode(buffer))

  def decode(buffer: BufferReader): T
}

object Data {
  val statMinimumSize = 68

  def parseACL(str: String): Array[ACL] = {
    val aclTab = str.split(",")
    val aclList: Array[ACL] = new Array[ACL](aclTab.length)
    println(aclTab(0))

    for (acl <- aclTab) {
      val firstColon = acl.indexOf(":")
      val lastColon = acl.lastIndexOf(":")
      println(firstColon + " " + lastColon)
      assert(!(firstColon == -1 || lastColon == -1 || firstColon == lastColon), "does not have the form scheme:id:perm")

      val newACL = new ACL(getPermFromString(acl.substring(lastColon + 1)),
        new ID(acl.substring(0, firstColon),
          acl.substring(firstColon + 1,
            lastColon)))
      aclList :+ newACL
    }
    aclList
  }

  def getPermFromString(permString: String): Int = {
    var perm: Int = 0
    permString.map(char => {
      char match {
        case 'r' => perm = perm | Perms.READ
        case 'w' => perm = perm | Perms.WRITE
        case 'c' => perm = perm | Perms.CREATE
        case 'd' => perm = perm | Perms.DELETE
        case 'a' => perm = perm | Perms.ADMIN
        case _ => println("Unknown perm type")
      }
    })
    perm
  }
}

case class Stat(czxid: Long,
                mzxid: Long,
                ctime: Long,
                mtime: Long,
                version: Int,
                cversion: Int,
                aversion: Int,
                ephemeralOwner: Long,
                dataLength: Int,
                numChildren: Int,
                pzxid: Long) extends Data with serializable {
  override def toBuffer: Buffer = {
    val bw = BufferWriter(Buffer.getDynamicBuffer(Data.statMinimumSize))
    bw.write(Data.statMinimumSize)
    bw.write(czxid)
    bw.write(mzxid)
    bw.write(ctime)
    bw.write(mtime)
    bw.write(version)
    bw.write(cversion)
    bw.write(aversion)
    bw.write(ephemeralOwner)
    bw.write(dataLength)
    bw.write(numChildren)
    bw.write(pzxid)

    bw
  }
}

case class ACL(perms: Int, id: ID) extends Data

case class ID(scheme: String, id: String) extends Data

object Perms {
  final val READ: Int = 1 << 0
  final val WRITE: Int = 1 << 1
  final val CREATE: Int = 1 << 2
  final val DELETE: Int = 1 << 3
  final val ADMIN: Int = 1 << 4
  final val ALL: Int = READ | WRITE | CREATE | DELETE | ADMIN
}

object ACL extends DecoderData[ACL] {
  val defaultACL = Array.fill(1)(new ACL(31, new ID("world", "anyone")))

  def apply(perm: Int, scheme: String, id: String): ACL = {
    new ACL(perm, new ID(scheme, id))
  }

  override def decode(buffer: BufferReader): ACL = {
    ACL(buffer.readInt, buffer.readString, buffer.readString)
  }

  def decodeArray(buffer: BufferReader): Array[ACL] = {
    val size = buffer.readInt
    val aclList = new Array[ACL](size)

    for (i <- 0 until size) {
      aclList(i) = decode(buffer)
    }

    aclList
  }
}

object Stat extends DecoderData[Stat] {
  override def decode(buffer: BufferReader): Stat = {

    val czxid = buffer.readLong
    val mzxid = buffer.readLong
    val ctime = buffer.readLong
    val mtime = buffer.readLong
    val version = buffer.readInt
    val cversion = buffer.readInt
    val aversion = buffer.readInt
    val ephemeralOwner = buffer.readLong
    val dataLength = buffer.readInt
    val numChildren = buffer.readInt
    val pzxid = buffer.readLong

    new Stat(czxid, mzxid, ctime, mtime, version, cversion, aversion, ephemeralOwner, dataLength, numChildren, pzxid)
  }
}