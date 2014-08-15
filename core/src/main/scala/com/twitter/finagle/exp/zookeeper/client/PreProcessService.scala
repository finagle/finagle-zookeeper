package com.twitter.finagle.exp.zookeeper.client

import com.twitter.concurrent.{AsyncSemaphore, Permit}
import com.twitter.finagle.{CancelledRequestException, Service}
import com.twitter.finagle.exp.zookeeper._
import com.twitter.finagle.exp.zookeeper.client.managers.ClientManager
import com.twitter.finagle.exp.zookeeper.connection.ConnectionManager
import com.twitter.finagle.exp.zookeeper.session.SessionManager
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util._
import java.util.concurrent.atomic.AtomicBoolean

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
  zkClient: ZkClient with ClientManager
) extends Service[Request, RepPacket] {

  private[this] val semaphore = new AsyncSemaphore(1)
  private[this] var permit: Option[Permit] = None
  private[this] val cancelAllRequests: AtomicBoolean = new AtomicBoolean(false)

  /**
   * Should send a request to the dispatcher, this request will be checked
   * by isROCheck before, to make sure this is not a Write operation and
   * that we are currently on read-only mode. Then the connection and
   * session are possibly checked with tryCheckLink depending if we
   * are already trying to reconnect. Next the request is prepared :
   * transformed to a ReqPacket, by adding opCode and xid. It is finally
   * sent to the service owned by the current connection object.
   *
   * @param req a Request
   * @return a Future[RepPacket] in response to the request
   */
  def apply(req: Request): Future[RepPacket] = {
    isROCheck(req) match {
      case Return(unit) =>
        zkClient.tryCheckLink()
        val p = new Promise[RepPacket]

        semaphore.acquire() respond {
          case Return(requestPermit) =>
            if (cancelAllRequests.get()) {
              p.updateIfEmpty(Throw(new CancelledRequestException))
              requestPermit.release()
            }
            else sendRequest(req, p, requestPermit)

          case Throw(exc) => p.setException(exc)
        }
        p

      case Throw(exc) => Future.exception(exc)
    }
  }

  private[this] def sendRequest(
    req: Request,
    p: Promise[RepPacket],
    requestPermit: Permit
  ): Future[Unit] = {
    implicit val timer = DefaultTimer.twitter
    val preparedReq = prepareRequest(req)
    connectionManager.connection.get.serve(preparedReq)
      .raiseWithin(sessionManager.session.negotiatedTimeout * 2 / 3)
      .respond {
      case Return(rep) => p.setValue(rep)

      case Throw(exc) => exc match {
        case exc1: TimeoutException =>
          zkClient.stopJob() before {
            if (zkClient.autoReconnect) zkClient.reconnectWithSession().unit
            else Future.Done
          } before {
            p.updateIfEmpty(Throw(exc1))
            Future.Done
          }

        case _ => p.updateIfEmpty(Throw(exc))
      }
    }.unit ensure requestPermit.release()
  }

  /**
   * Should lock the service until unlockServe() is called, it's done
   * simply by acquiring the semaphore permit.
   *
   * @return Future.Done
   */
  private[finagle] def lockService(): Future[Unit] = this.synchronized {
    if (permit.isDefined) Future.Done
    else semaphore.acquire() flatMap { perm =>
      permit = Some(perm)
      Future.Done
    }
  }

  /**
   * Should unlock the service after lockServe() was called, it's done
   * simply by releasing the semaphore permit.
   *
   */
  private[finagle] def unlockService(): Unit = this.synchronized {
    if (permit.isDefined) {
      permit.get.release()
      permit = None
    }
  }

  /**
   * Should cancel all the pending requests and lock the service
   *
   * @return Future.Done when the action is completed
   */
  private[finagle] def flushService(): Future[Unit] = this.synchronized {
    cancelAllRequests.set(true)
    unlockService()
    lockService()
  } ensure cancelAllRequests.set(false)

  /**
   * Should prepare a request by adding xid and request's op code
   *
   * @param req the request to prepare
   * @return a ReqPacket
   */
  private[this] def prepareRequest(req: Request): ReqPacket =
    Request.toReqPacket(req, sessionManager.session.nextXid)

  /**
   * Should check if the request is a write operation and throw an
   * exception if the server is in Read Only mode, because only read
   * operations are allowed during this state.
   *
   * @param req the request to test
   * @return
   */
  private[this] def isROCheck(req: Request): Try[Unit] = {
    lazy val isro = sessionManager.session.isRO.get()

    def testRO(): Try[Unit] =
      if (isro) actOnRO()
      else Return.Unit

    def actOnRO(): Try[Unit] = {
      Throw(NotReadOnlyException(
        "Server is in ReadOnly mode, write requests are not allowed"))
    }

    req match {
      case req: CreateRequest => testRO()
      case req: Create2Request => testRO()
      case req: DeleteRequest => testRO()
      case req: ReconfigRequest => testRO()
      case req: SetACLRequest => testRO()
      case req: SetDataRequest => testRO()
      case req: SyncRequest => testRO()
      case req: TransactionRequest => testRO()
      case req: Request => Return.Unit
    }
  }
}