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
    Buf.U32BE(arrBuf.length).concat(arrBuf)
  }

  def unapply(buf: Buf): Option[(Array[Byte], Buf)] = {
    val Buf.U32BE(len, rem) = buf
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

private[finagle] object BufSeq {
  def apply[T](s: Seq[T], toBuf: T => Buf): Buf =
    s.foldLeft(Buf.U32BE(s.size)) { (b, i) => b.concat(toBuf(i)) }

  def unapply[T](x: (Buf, Buf => Option[(T, Buf)])): Option[(Seq[T], Buf)] = {
    val (buf, fromBuf) = x

    var rem: Buf = Buf.Empty
    val Buf.U32BE(len, r) = buf
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
    Buf.U32BE(strBuf.length).concat(strBuf)
  }

  def unapply(buf: Buf): Option[(String, Buf)] = {
    val Buf.U32BE(len, rem) = buf
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