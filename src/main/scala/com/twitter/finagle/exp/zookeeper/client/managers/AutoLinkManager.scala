package com.twitter.finagle.exp.zookeeper.client.managers

import java.util.concurrent.atomic.AtomicBoolean

import com.twitter.finagle.exp.zookeeper.client.ZkClient
import com.twitter.finagle.exp.zookeeper.session.Session.States
import com.twitter.finagle.exp.zookeeper.{SessionMovedException, ZookeeperException}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.{Duration, TimerTask, Future}

trait AutoLinkManager {self: ZkClient with ClientManager =>

  private[this] val canCheckLink = new AtomicBoolean(false)
  /**
   * Start the loop which will check connection and session
   * every timeBetweenLinkCheck
   * @return Future.Done
   */
  def startStateLoop(): Unit =
    if (autoReconnect) {
      require(timeBetweenAttempts.isDefined)
      require(timeBetweenLinkCheck.isDefined)
      require(maxConsecutiveRetries.isDefined)
      require(maxReconnectAttempts.isDefined)

      this.synchronized {
        canCheckLink.set(true)
        if (!CheckLingScheduler.isRunning) {
          CheckLingScheduler(timeBetweenLinkCheck.get)(tryCheckLink())
        }
      }
    }

  def stopStateLoop(): Unit = {
    canCheckLink.set(false)
    CheckLingScheduler.stop()
  }

  private[finagle] def tryCheckLink(): Unit =
    if (canCheckLink.get()) checkLink()


  /**
   * Check session state before checking connection state
   * @param tries number of reconnect attempts
   * @return Future.Done or exception
   */
  private[this] def checkLink(tries: Int = 0): Future[Unit] = this.synchronized {
    checkSession() rescue {
      case exc: SessionMovedException => Future.exception(exc)
      case exc: ZookeeperException =>
        if (tries < maxReconnectAttempts.get)
          checkLink(tries + 1)
            .delayed(timeBetweenAttempts.get)(DefaultTimer.twitter)
        else Future.exception(exc)
      case exc: Throwable => Future.exception(exc)
    } before checkConnection() rescue {
      case exc: ZookeeperException =>
        if (tries < maxReconnectAttempts.get)
          checkLink(tries + 1)
            .delayed(timeBetweenAttempts.get)(DefaultTimer.twitter)
        else Future.exception(exc)
      case exc: Throwable => Future.exception(exc)
    }
  }

  /**
   * To check that connection is still valid
   * @return Future.Done or exception
   */
  private[this] def checkConnection(): Future[Unit] =
    if (connectionManager.connection.isDefined &&
      !connectionManager.connection.get.isValid.get()) {
      stopJob() before reconnectWithSession()
    } else Future.Done

  /**
   * This method is called to make sure the connection is still alive.
   * If it's not then it can try to reconnect or create a new
   * session if the current one has expired.
   * It won't connect if the client has never connected
   */
  private[this] def checkSession(): Future[Unit] =
    sessionManager.session.state match {
      case States.CONNECTION_LOSS | States.NOT_CONNECTED =>
        stopJob() before reconnectWithSession()

      case States.SESSION_MOVED => stopJob() before
        Future.exception(SessionMovedException(
          "Session has moved to another server"))

      case States.SESSION_EXPIRED => stopJob() before reconnectWithoutSession()
      case _ => Future.Done
    }

  /**
   * PreventiveSearchScheduler is used to find a suitable server to reconnect
   * to in case of server failure.
   */
  private[this] object CheckLingScheduler {
    /**
     * currentTask - the last scheduled timer's task
     */
    private[this] var currentTask: Option[TimerTask] = None

    def apply(period: Duration)(f: => Unit) {
      currentTask = Some(DefaultTimer.twitter.schedule(period)(f))
    }

    def isRunning: Boolean = { currentTask.isDefined }

    def stop() {
      if (currentTask.isDefined) currentTask.get.cancel()
      currentTask = None
    }

    def updateTimer(period: Duration)(f: => Unit) = {
      stop()
      apply(period)(f)
    }
  }
}