package com.twitter.finagle.exp.zookeeper.client.managers

import com.twitter.concurrent.{AsyncSemaphore, Permit}
import com.twitter.finagle.Service
import com.twitter.finagle.exp.zookeeper._
import com.twitter.finagle.exp.zookeeper.connection.ConnectionManager
import com.twitter.finagle.exp.zookeeper.session.SessionManager
import com.twitter.util._

/**
 * LocalService is used to send ZooKeeper request to the endpoint.
 * In client life, connection or session changes can happen,
 * during those moments we are not able to send request because
 * either the connection is down or the session is not established.
 * For these reasons, during connection/reconnection/changeHost
 * we can lock the service until the situation is back to normal.
 */
class PreProcessService(
  connectionManager: ConnectionManager,
  sessionManager: SessionManager,
  linkChecker: AutoLinkManager
  ) extends Service[Request, RepPacket] {

  // todo use raiseWithin for requests timeout
  private[this] val semaphore = new AsyncSemaphore(1)
  private[this] var permit: Option[Permit] = None

  def apply(req: Request): Future[RepPacket] = {
    isROCheck(req) match {
      case Return(unit) =>
        linkChecker.tryCheckLink()
        val p = new Promise[RepPacket]

        semaphore.acquire() onSuccess { permit =>
          val preparedReq = prepareRequest(req)
          connectionManager.connection.get.serve(preparedReq) respond {
            case Throw(exc) =>
              p.updateIfEmpty(Throw(exc))
              permit.release()
            case Return(rep) =>
              p.setValue(rep)
              permit.release()
          }
        } onFailure { p.setException }
        p

      case Throw(exc) => Future.exception(exc)
    }
  }

  /**
   * Should lock the service until unlockServe() is called
   * @return Future.Done
   */
  private[finagle] def lockServe(): Future[Unit] = this.synchronized {
    if (permit.isDefined) Future.Done
    else semaphore.acquire() flatMap { perm =>
      permit = Some(perm)
      Future.Done
    }
  }

  /**
   * Should unlock the service after lockServe() was called
   * @return Future.Done
   */
  private[finagle] def unlockServe(): Future[Unit] = this.synchronized {
    if (permit.isDefined) {
      permit.get.release()
      permit = None
      Future.Done
    } else Future.Done
  }

  /**
   * Should prepare a request by adding xid and op code
   * @param req the request to prepare
   * @return a ReqPacket
   */
  private[this] def prepareRequest(req: Request): ReqPacket =
    Request.toReqPacket(req, sessionManager.session.nextXid)

  /**
   * Should check if the request is a write operation and throw an
   * exception if the server is in Read Only mode, because only read
   * operations are allowed during this state.
   * @param req the request to test
   * @return
   */
  private[this] def isROCheck(req: Request): Try[Unit] = {
    val isro = sessionManager.session.isRO.get()

    def testRO(): Try[Unit] =
      if (isro) actOnRO()
      else Return.Unit

    def actOnRO(): Try[Unit] = {
      Throw(NotReadOnlyException(
        "Server is in ReadOnly mode, write requests are not allowed"))
    }

    req match {
      case req: CreateRequest => testRO()
      case req: DeleteRequest => testRO()
      case req: SetACLRequest => testRO()
      case req: SetDataRequest => testRO()
      case req: SyncRequest => testRO()
      case req: TransactionRequest => testRO()
      case req: Request => Return.Unit
    }
  }
}