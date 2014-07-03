package com.twitter.finagle.exp.zookeeper.transport

import com.twitter.finagle.exp.zookeeper.data.{Id, ACL}
import com.twitter.io.Buf

private[finagle] object BufArray {
  def toBytes(buf: Buf): Array[Byte] = {
    val bytes = new Array[Byte](buf.length)
    buf.write(bytes, 0)
    bytes
  }

  def apply(a: Array[Byte]): Buf = {
    val arrBuf = Buf.ByteArray(a)
    BufInt(arrBuf.length).concat(arrBuf)
  }

  def unapply(buf: Buf): Option[(Array[Byte], Buf)] = {
    val BufInt(len, rem) = buf
    Some(toBytes(rem.slice(0, len)), rem.slice(len, rem.length))
  }
}

private[finagle] object BufBool {
  def apply(b: Boolean): Buf = {
    Buf.ByteArray((if (b) 1 else 0).toByte)
  }

  def unapply(buf: Buf): Option[(Boolean, Buf)] = {
    val bytes = new Array[Byte](1)
    buf.slice(0, 1).write(bytes, 0)
    val rem = buf.slice(1, buf.length)

    if (bytes(0) < 0) None else Some(bytes(0) != 0, rem)
  }
}

private[finagle] object BufInt {
  def apply(i: Int): Buf = {
    val arr = new Array[Byte](4)
    arr(0) = ((i >> 24) & 0xff).toByte
    arr(1) = ((i >> 16) & 0xff).toByte
    arr(2) = ((i >> 8) & 0xff).toByte
    arr(3) = (i & 0xff).toByte
    Buf.ByteArray(arr)
  }

  def unapply(buf: Buf): Option[(Int, Buf)] = {
    val arr = new Array[Byte](4)
    buf.slice(0, 4).write(arr, 0)

    val out: Int =
      ((arr(0) & 0xff) << 24) |
        ((arr(1) & 0xff) << 16) |
        ((arr(2) & 0xff) << 8) |
        (arr(3) & 0xff)
    Some(out, buf.slice(4, buf.length))
  }
}

private[finagle] object BufLong {
  def apply(l: Long): Buf = {
    val arr = new Array[Byte](8)
    arr(0) = ((l >> 56) & 0xff).toByte
    arr(1) = ((l >> 48) & 0xff).toByte
    arr(2) = ((l >> 40) & 0xff).toByte
    arr(3) = ((l >> 32) & 0xff).toByte
    arr(4) = ((l >> 24) & 0xff).toByte
    arr(5) = ((l >> 16) & 0xff).toByte
    arr(6) = ((l >> 8) & 0xff).toByte
    arr(7) = (l & 0xff).toByte
    Buf.ByteArray(arr)
  }

  def unapply(buf: Buf): Option[(Long, Buf)] = {
    val arr = new Array[Byte](8)
    buf.slice(0, 8).write(arr, 0)

    val out: Long =
      ((arr(0) & 0xff).toLong << 56) |
        ((arr(1) & 0xff).toLong << 48) |
        ((arr(2) & 0xff).toLong << 40) |
        ((arr(3) & 0xff).toLong << 32) |
        ((arr(4) & 0xff).toLong << 24) |
        ((arr(5) & 0xff).toLong << 16) |
        ((arr(6) & 0xff).toLong << 8) |
        (arr(7) & 0xff).toLong
    Some(out, buf.slice(8, buf.length))
  }
}

private[finagle] object BufSeq {
  def apply[T](s: Seq[T], toBuf: T => Buf): Buf =
    s.foldLeft(BufInt(s.size)) { (b, i) => b.concat(toBuf(i)) }

  def unapply[T](x: (Buf, Buf => Option[(T, Buf)])): Option[(Seq[T], Buf)] = {
    val (buf, fromBuf) = x

    var rem: Buf = Buf.Empty
    val BufInt(len, r) = buf
    rem = r

    val items = (0 until len) flatMap { _ =>
      fromBuf(rem) map { case (i, j) =>
        rem = j
        i
      }
    }

    Some(items, rem)
  }
}

private[finagle] object BufString {
  def apply(s: String): Buf = {
    val strBuf = Buf.Utf8(s)
    BufInt(strBuf.length).concat(strBuf)
  }

  def unapply(buf: Buf): Option[(String, Buf)] = {
    val BufInt(len, rem) = buf
    val Buf.Utf8(str) = rem.slice(0, len)
    Some(str, rem.slice(len, rem.length))
  }
}

private[finagle] object BufSeqACL {
  def apply(s: Seq[ACL]): Buf = BufSeq[ACL](s, _.buf)
  def unapply(buf: Buf): Option[(Seq[ACL], Buf)] =
    BufSeq.unapply[ACL]((buf, ACL.unapply))
}

private[finagle] object BufSeqId {
  def apply(s: Seq[Id]): Buf = BufSeq[Id](s, _.buf)
  def unapply(buf: Buf): Option[(Seq[Id], Buf)] =
    BufSeq.unapply[Id]((buf, Id.unapply))
}

private[finagle] object BufSeqString {
  def apply(s: Seq[String]): Buf = BufSeq[String](s, BufString.apply)
  def unapply(buf: Buf): Option[(Seq[String], Buf)] =
    BufSeq.unapply[String]((buf, BufString.unapply))
}