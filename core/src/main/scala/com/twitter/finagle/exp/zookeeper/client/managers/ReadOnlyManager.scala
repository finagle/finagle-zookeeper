package com.twitter.finagle.exp.zookeeper.client.managers

import com.twitter.finagle.exp.zookeeper.client.ZkClient
import com.twitter.util.{Future, Return, Throw}
import java.util.concurrent.atomic.AtomicBoolean

private[finagle] trait ReadOnlyManager {self: ZkClient with ClientManager =>
  private[this] val canSearch = new AtomicBoolean(true)

  /**
   * Should search a RW mode server and connect to it.
   *
   * @return Future.Done
   */
  private[this] def findAndConnectRwServer(): Future[Unit] = {
    ZkClient.logger.info("Client has started looking for a Read-Write server in background.")
    connectionManager.hostProvider.startRwServerSearch() transform {
      case Return(server) =>
        if (sessionManager.session.isRO.get() && canSearch.get()) {
          if (sessionManager.session.hasFakeSessionId.get) {
            ZkClient.logger.info(
              ("Client has found a Read-Write server" +
                " at %s, now reconnecting to it" +
                " without session.").format(server))
            reconnectWithoutSession(Some(server)).unit
          }
          else {
            ZkClient.logger.info(
              ("Client has found a Read-Write server" +
                " at %s, now reconnecting to it" +
                " with session.").format(server))
            reconnectWithSession(Some(server)).unit
          }
        } else Future.Done

      case Throw(exc) =>
        if (sessionManager.session.isRO.get() && canSearch.get())
          findAndConnectRwServer()
        else Future.Done
    }
  }

  /**
   * Should start to search a RW mode server.
   *
   * @return Future.Done
   */
  def startRwSearch(): Future[Unit] =
    if (canSearch.getAndSet(false)) findAndConnectRwServer()
    else Future.Done

  /**
   * Should stop Rw mode server search.
   *
   * @return Future.Done
   */
  def stopRwSearch(): Future[Unit] =
    if (!canSearch.getAndSet(true))
      connectionManager.hostProvider.stopRwServerSearch()
    else Future.Done
}