package com.twitter.finagle.exp.zookeeper

import com.twitter.finagle.exp.zookeeper.transport._
import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.OpCode
import com.twitter.finagle.exp.zookeeper.data.{ACL, Stat}
import com.twitter.io.Buf
import scala.annotation.tailrec
import com.twitter.util.{Throw, Return, Try}

sealed trait OpResult
sealed trait OpRequest {
  def buf: Buf
}

trait ResultDecoder[U <: OpResult] {
  def unapply(buffer: Buf): Option[(U, Buf)]
  def apply(buffer: Buf): Try[(U, Buf)] = Try {
    unapply(buffer) match {
      case Some((rep, rem)) => (rep, rem)
      case None => throw ZkDecodingException("Error while decoding")
    }
  }
}

class Transaction(opList: Seq[OpRequest]) {
  def buf: Buf =
    opList.foldLeft(Buf.Empty)((buf, op) => buf.concat(op.buf))
      .concat(MultiHeader(-1, true, -1).buf)
}

/**
 * A MultiHeader is used to describe an operation
 * @param typ type of the operation
 * @param state state
 * @param err error
 */
case class MultiHeader(typ: Int, state: Boolean, err: Int)
  extends OpRequest with OpResult {
  def buf: Buf = Buf.Empty
    .concat(BufInt(typ))
    .concat(BufBool(state))
    .concat(BufInt(err))
}

/**
 * A result from a create operation.  This kind of result allows the
 * path to be retrieved since the create might have been a sequential
 * create.
 */
case class CreateResult(path: String) extends OpResult
/**
 * An error result from any kind of operation.  The point of error results
 * is that they contain an error code which helps understand what happened.
 * @see ZookeeperExceptions
 */
case class ErrorResult(errorCode: Int) extends OpResult
/**
 * A result from a delete operation.  No special values are available.
 */
class DeleteResult() extends OpResult
/**
 * A result from a setData operation.  This kind of result provides access
 * to the Stat structure from the update.
 */
case class SetDataResult(stat: Stat) extends OpResult
/**
 * A result from a version check operation.  No special values are available.
 */
class CheckResult() extends OpResult

case class Create2Result(path: String, stat: Stat) extends OpResult

case class CreateOp(
  path: String,
  data: Array[Byte],
  aclList: Array[ACL],
  createMode: Int)
  extends OpRequest {
  def buf: Buf = Buf.Empty
    .concat(MultiHeader(OpCode.CREATE, false, -1).buf)
    .concat(BufString(path))
    .concat(BufArray(data))
    .concat(BufSeqACL(aclList))
    .concat(BufInt(createMode))
}

case class DeleteOp(path: String, version: Int)
  extends OpRequest {
  def buf: Buf = Buf.Empty
    .concat(MultiHeader(OpCode.DELETE, false, -1).buf)
    .concat(BufString(path))
    .concat(BufInt(version))
}

case class SetDataOp(path: String, data: Array[Byte], version: Int)
  extends OpRequest {
  def buf: Buf = Buf.Empty
    .concat(MultiHeader(OpCode.SET_DATA, false, -1).buf)
    .concat(BufString(path))
    .concat(BufArray(data))
    .concat(BufInt(version))
}

case class CheckOp(path: String, version: Int)
  extends OpRequest {
  def buf: Buf = Buf.Empty
    .concat(MultiHeader(OpCode.CHECK, false, -1).buf)
    .concat(BufString(path))
    .concat(BufInt(version))
}


object Transaction {
  def unapply(buf: Buf): Option[(TransactionResponse, Buf)] = {

    Try(decode(buf, Seq.empty[OpResult])) match {
      case Return(res) => Some((TransactionResponse(res._1), res._2))
      case Throw(exc) => None
    }
  }

  @tailrec
  private[this] def decode(buf: Buf, results: Seq[OpResult]): (Seq[OpResult], Buf) = {
    val MultiHeader(header, opBuf) = buf
    if (header.state) (results, opBuf)
    else {
      val (res, rem) = header.typ match {
        case OpCode.CREATE2 =>
          val Create2Result(rep, rem) = opBuf
          (Create2Result(rep.path, rep.stat), rem)

        case OpCode.DELETE =>
          (new DeleteResult, opBuf)

        case OpCode.SET_DATA =>
          val SetDataResult(rep, rem) = opBuf
          (SetDataResult(rep.stat), rem)

        case OpCode.CHECK =>
          (new CheckResult, opBuf)

        case OpCode.ERROR =>
          val ErrorResponse(rep, rem) = opBuf
          (ErrorResult(rep.err), rem)

        case typ =>
          throw new Exception("Invalid type %d in MultiResponse".format(typ))
      }
      decode(rem, results :+ res)
    }
  }
}

object MultiHeader extends ResultDecoder[MultiHeader] {
  def unapply(buf: Buf): Option[(MultiHeader, Buf)] = {
    val BufInt(typ, BufBool(done, BufInt(err, rem))) = buf
    Some(MultiHeader(typ, done, err), rem)
  }
}

object CreateResult extends ResultDecoder[CreateResult] {
  def unapply(buf: Buf): Option[(CreateResult, Buf)] = {
    val BufString(path, rem) = buf
    Some(CreateResult(path), rem)
  }
}

object ErrorResult extends ResultDecoder[ErrorResult] {
  def unapply(buf: Buf): Option[(ErrorResult, Buf)] = {
    val BufInt(errorCode, rem) = buf
    Some(ErrorResult(errorCode), rem)
  }
}

object SetDataResult extends ResultDecoder[SetDataResult] {
  def unapply(buf: Buf): Option[(SetDataResult, Buf)] = {
    val Stat(stat, rem) = buf
    Some(SetDataResult(stat), rem)
  }
}

object Create2Result extends ResultDecoder[Create2Result] {
  def unapply(buf: Buf): Option[(Create2Result, Buf)] = {
    val BufString(path, Stat(stat, rem)) = buf
    Some(Create2Result(path, stat), rem)
  }
}