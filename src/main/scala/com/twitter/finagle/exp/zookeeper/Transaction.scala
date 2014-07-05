package com.twitter.finagle.exp.zookeeper

import com.twitter.finagle.exp.zookeeper.data.ACL
import com.twitter.finagle.exp.zookeeper.transport._
import com.twitter.finagle.exp.zookeeper.utils.PathUtils
import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.OpCode
import com.twitter.io.Buf
import com.twitter.util.{Return, Throw, Try}
import scala.annotation.tailrec

/**
 * A MultiHeader is used to describe an operation
 *
 * @param typ type of the operation
 * @param state state
 * @param err error
 */
private[finagle] case class MultiHeader(typ: Int, state: Boolean, err: Int)
  extends ReqHeader with RepHeader {
  def buf: Buf = Buf.Empty
    .concat(Buf.U32BE(typ))
    .concat(BufBool(state))
    .concat(Buf.U32BE(err))
}

private[finagle] object MultiHeader extends {
  def unapply(buf: Buf): Option[(MultiHeader, Buf)] = {
    val Buf.U32BE(typ, BufBool(done, Buf.U32BE(err, rem))) = buf
    Some(MultiHeader(typ, done, err), rem)
  }
}

private[finagle] object Transaction {
  /**
   * Should decode a multi operation request (ie Transaction) by
   * decomposing the buf until we find the "end multiheader" acting
   * as a delimiter.
   *
   * @param results the OpResult sequence to use
   * @param buf the Buf to read
   * @return (Seq[OpResult], Buf)
   */
  @tailrec
  def decode(
    results: Seq[OpResult],
    buf: Buf
    ): (Seq[OpResult], Buf) = {

    val MultiHeader(header, opBuf) = buf
    if (header.state) (results, opBuf)
    else {
      val (res, rem) = header.typ match {
        case OpCode.CREATE =>
          val CreateResponse(rep, rem) = opBuf
          (CreateResponse(rep.path), rem)

        case OpCode.CREATE2 =>
          val Create2Response(rep, rem) = opBuf
          (Create2Response(rep.path, rep.stat), rem)

        case OpCode.DELETE => (new EmptyResponse, opBuf)

        case OpCode.SET_DATA =>
          val SetDataResponse(rep, rem) = opBuf
          (SetDataResponse(rep.stat), rem)

        case OpCode.CHECK => (new EmptyResponse, opBuf)

        case OpCode.ERROR =>
          val ErrorResponse(rep, rem) = opBuf
          (ErrorResponse(rep.exception), rem)

        case _ => throw new IllegalArgumentException(
          "Unsupported type of operation result while decoding Transaction")
      }
      decode(results :+ res, rem)
    }
  }

  /**
   * Should prepare a Transaction request by checking each operation :
   * check ACL, prepend chroot and validate path.
   *
   * @param opList the operation list to prepare
   * @param chroot the client's chroot
   * @return configured operation list
   */
  def prepareAndCheck(
    opList: Seq[OpRequest],
    chroot: String
    ): Try[Seq[OpRequest]] = {

    Try.collect(opList map {
      case op: CreateRequest =>
        ACL.check(op.aclList)
        val finalPath = PathUtils.prependChroot(op.path, chroot)
        PathUtils.validatePath(finalPath, op.createMode)
        Return(CreateRequest(finalPath, op.data, op.aclList, op.createMode))

      case op: Create2Request =>
        ACL.check(op.aclList)
        val finalPath = PathUtils.prependChroot(op.path, chroot)
        PathUtils.validatePath(finalPath, op.createMode)
        Return(Create2Request(finalPath, op.data, op.aclList, op.createMode))

      case op: DeleteRequest =>
        val finalPath = PathUtils.prependChroot(op.path, chroot)
        PathUtils.validatePath(finalPath)
        Return(DeleteRequest(finalPath, op.version))

      case op: SetDataRequest =>
        require(op.data.size < 1048576,
          "The maximum allowable size of " +
            "the data array is 1 MB (1,048,576 bytes)")
        val finalPath = PathUtils.prependChroot(op.path, chroot)
        PathUtils.validatePath(finalPath)
        Return(SetDataRequest(finalPath, op.data, op.version))

      case op: CheckVersionRequest =>
        val finalPath = PathUtils.prependChroot(op.path, chroot)
        PathUtils.validatePath(finalPath)
        Return(CheckVersionRequest(finalPath, op.version))

      case _ =>
        Throw(new IllegalArgumentException("Element is not an Op"))
    })
  }

  /**
   * Should correctly format each operation response path by removing chroot
   * from the returned path.
   *
   * @param opList the operation result to format
   * @param chroot the client's chroot
   * @return correctly formatted operation list
   */
  def formatPath(opList: Seq[OpResult], chroot: String): Seq[OpResult] =
    opList map {
      case op: Create2Response =>
        Create2Response(op.path.substring(chroot.length), op.stat)
      case op: CreateResponse =>
        CreateResponse(op.path.substring(chroot.length))
      case op: OpResult => op
    }
}