package com.twitter.finagle.exp.zookeeper.client.managers

import com.twitter.finagle.CancelledRequestException
import com.twitter.finagle.exp.zookeeper.client.ZkClient
import com.twitter.util.{Future, Promise, Return, Throw}

private[finagle] trait ReadOnlyManager {self: ZkClient with ClientManager =>
  private[this] var rwSearchOp: Option[Future[Unit]] = None

  /**
   * Should search a RW mode server and connect to it.
   *
   * @return Future.Done
   */
  private[this] def findAndConnectRwServer(): Future[Unit] = {
    var interrupt = false
    var search: Future[String] = new Promise[String]()
    val p = new Promise[Unit]()
    p.setInterruptHandler {
      case exc: CancelledRequestException =>
        interrupt = true
        search.raise(new CancelledRequestException)
        p.setDone()
      case exc =>
        p.updateIfEmpty(Throw(exc))
    }

    ZkClient.logger.info(
      "Client has started looking for a Read-Write server in background.")

    if (!interrupt) {
      search = connectionManager.hostProvider.findRwServer(timeBetweenRwSrch.get)
      search transform {
        case Return(server) if sessionManager.session.isRO.get() && !interrupt =>
          if (sessionManager.session.hasFakeSessionId.get) {
            ZkClient.logger.info(
              "Client has found a Read-Write server" +
                s" at $server, now reconnecting to it" +
                " without session.")
            reconnectWithoutSession(Some(server)).unit
          }
          else {
            ZkClient.logger.info(
              "Client has found a Read-Write server" +
                s" at $server, now reconnecting to it" +
                " with session.")
            reconnectWithSession(Some(server)).unit
          }

        case _ => Future.Done
      }

      p.updateIfEmpty(Return(search))
    }
    else p.setDone()

    p
  }

  /**
   * Should start to search a RW mode server.
   *
   * @return Future.Done
   */
  private[finagle] def startRwSearch(): Future[Unit] =
    stopRwSearch() onSuccess { _ =>
      rwSearchOp = Some(findAndConnectRwServer())
    }

  /**
   * Should stop Rw mode server search.
   *
   * @return Future.Done
   */
  private[finagle] def stopRwSearch(): Future[Unit] =
    if (rwSearchOp.isDefined) {
      rwSearchOp.get.raise(new CancelledRequestException)
      rwSearchOp.get
    }
    else Future.Done
}