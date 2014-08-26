package com.twitter.finagle.exp.zookeeper.example

import com.twitter.finagle.exp.zookeeper.{ConnectionLossException, NoNodeException}
import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.CreateMode
import com.twitter.finagle.exp.zookeeper.client.ZkClient
import com.twitter.finagle.exp.zookeeper.data.Ids
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.{Throw, Return, Duration, Future}

/**
 * Lock recipe with finagle-zookeeper.
 *
 * @param timeout duration between each retry attempts
 * @param rootLockPath base root path for locks
 * @param lockNodePrefix prefix name for lock nodes
 * @param client a connected ZkClient
 */
class Locks(
  timeout: Duration,
  rootLockPath: String,
  lockNodePrefix: String
)(implicit client: ZkClient) {

  implicit val timer = DefaultTimer.twitter
  val lockPathModel = rootLockPath + "/" + lockNodePrefix + "-"
  // In this example we suppose you are already connected to a server
  // Session management is up to the caller

  // Create a node for all the lock nodes
  def initiate(): Future[Unit] =
    client.create(
      rootLockPath,
      "root node for locks".getBytes,
      Ids.OPEN_ACL_UNSAFE,
      CreateMode.PERSISTENT
    ).unit

  // Remove the base lock node
  def clean(): Future[Unit] = client.delete(rootLockPath, -1)

  def lockAndAct[A](f: => A): Future[A] = {
    lock() flatMap { lockpath =>
      val res = f
      releaseLock(lockpath) before Future(res)
    }
  }

  def lock(): Future[String] =
    client.create(
      lockPathModel,
      "".getBytes,
      Ids.OPEN_ACL_UNSAFE,
      CreateMode.EPHEMERAL_SEQUENTIAL
    ) flatMap obtainLeadership

  def releaseLock(lockNode: String): Future[Unit] = client.delete(lockNode, -1)

  /**
   * It will check that the node we created has the lowest id, if not
   * it will wait that the next lowest id node change.
   *
   * @param lockNode the created lock node
   * @return
   */
  private[this] def obtainLeadership(lockNode: String): Future[String] = {
    client.getChildren(rootLockPath) transform {
      case Return(getChildren) =>
        val createId = lockNode.drop(lockPathModel.size).toInt
        // ids of all the children
        val ids = getChildren.children.map { _.drop(lockNodePrefix.size + 1).toInt }

        if (ids.min == createId) Future(lockNode)
        else {
          client.exists(
            path = lockPathModel + ids.filter { _ < createId }.max,
            watch = true
          ) flatMap { ex =>
            if (ex.stat.isDefined)
              ex.watcher.get.event.unit before obtainLeadership(lockNode)

            else {
              // we don't need a watcher if node does not exist
              client.watcherManager.removeWatcher(ex.watcher.get)
              obtainLeadership(lockNode).delayed(timeout)
            }
          } rescue {
            // Temporary lock node does not exist, maybe we just reconnected to a server
            // calling lock while create a new ephemeral lock and call obtainLeadership
            case ex: NoNodeException => lock()

            // if autoreconnect is enabled, will try to reconnect
            // otherwise exception is propagated to the caller
            case ex: ConnectionLossException =>
              if (client.autoReconnect) obtainLeadership(lockNode)
              else throw ex

            case ex => throw ex
          }
        }

      // base lock node does not exist, will create and retry to obtain leadership
      case Throw(ex: NoNodeException) =>
        initiate() before obtainLeadership(lockNode)

      // we have lost the connection while getChildren
      // if autoreconnect is enabled, will try to reconnect
      // otherwise exception is propagated to the caller
      case Throw(ex: ConnectionLossException) =>
        if (client.autoReconnect) obtainLeadership(lockNode)
        else throw ex

      case Throw(ex) => throw ex
    }
  }
}