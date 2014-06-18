package com.twitter.finagle.exp.zookeeper.data

import com.twitter.finagle.exp.zookeeper.transport.{BufInt, BufLong}
import com.twitter.io.Buf

/**
 *
 * @param czxid The zxid of the change that caused this znode to be created.
 * @param mzxid The zxid of the change that last modified this znode.
 * @param ctime The time in milliseconds from epoch when this znode was created.
 * @param mtime The time in milliseconds from epoch when this znode was last modified.
 * @param version The number of changes to the data of this znode.
 * @param cversion The number of changes to the children of this znode.
 * @param aversion The number of changes to the ACL of this znode.
 * @param ephemeralOwner The session id of the owner of this znode if the znode is an ephemeral node. If it is not an ephemeral node, it will be zero.
 * @param dataLength The length of the data field of this znode.
 * @param numChildren The number of children of this znode.
 * @param pzxid Last modified children.
 */
case class Stat(
  czxid: Long,
  mzxid: Long,
  ctime: Long,
  mtime: Long,
  version: Int,
  cversion: Int,
  aversion: Int,
  ephemeralOwner: Long,
  dataLength: Int,
  numChildren: Int,
  pzxid: Long){
  def buf: Buf = Buf.Empty
    .concat(BufLong(czxid))
    .concat(BufLong(mzxid))
    .concat(BufLong(ctime))
    .concat(BufLong(mtime))
    .concat(BufInt(version))
    .concat(BufInt(cversion))
    .concat(BufInt(aversion))
    .concat(BufLong(ephemeralOwner))
    .concat(BufInt(dataLength))
    .concat(BufInt(numChildren))
    .concat(BufLong(pzxid))
}

object Stat {
  def unapply(buf: Buf): Option[(Stat, Buf)] = {
    val BufLong(czxid,
    BufLong(mzxid,
    BufLong(ctime,
    BufLong(mtime,
    BufInt(version,
    BufInt(cversion,
    BufInt(aversion,
    BufLong(ephemeralOwner,
    BufInt(dataLength,
    BufInt(numChildren,
    BufLong(pzxid,
    rem
    ))))))))))) = buf
    Some(Stat(czxid, mzxid, ctime, mtime, version, cversion, aversion, ephemeralOwner, dataLength, numChildren, pzxid), rem)
  }
}