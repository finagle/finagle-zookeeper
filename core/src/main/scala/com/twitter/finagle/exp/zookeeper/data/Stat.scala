package com.twitter.finagle.exp.zookeeper.data

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
  pzxid: Long) extends Data {
  def buf: Buf = Buf.Empty
    .concat(Buf.U64BE(czxid))
    .concat(Buf.U64BE(mzxid))
    .concat(Buf.U64BE(ctime))
    .concat(Buf.U64BE(mtime))
    .concat(Buf.U32BE(version))
    .concat(Buf.U32BE(cversion))
    .concat(Buf.U32BE(aversion))
    .concat(Buf.U64BE(ephemeralOwner))
    .concat(Buf.U32BE(dataLength))
    .concat(Buf.U32BE(numChildren))
    .concat(Buf.U64BE(pzxid))
}

private[finagle] object Stat extends DataDecoder[Stat] {
  def unapply(buf: Buf): Option[(Stat, Buf)] = buf match {
    case Buf.U64BE(czxid,
    Buf.U64BE(mzxid,
    Buf.U64BE(ctime,
    Buf.U64BE(mtime,
    Buf.U32BE(version,
    Buf.U32BE(cversion,
    Buf.U32BE(aversion,
    Buf.U64BE(ephemeralOwner,
    Buf.U32BE(dataLength,
    Buf.U32BE(numChildren,
    Buf.U64BE(pzxid,
    rem
    ))))))))))) =>
      Some(
        Stat(
          czxid,
          mzxid,
          ctime,
          mtime,
          version,
          cversion,
          aversion,
          ephemeralOwner,
          dataLength,
          numChildren,
          pzxid),
        rem)
    case _ => None
  }
}