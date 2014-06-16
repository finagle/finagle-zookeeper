package com.twitter.finagle.exp.zookeeper.session

import com.twitter.conversions.time._
import com.twitter.finagle.exp.zookeeper._
import com.twitter.finagle.exp.zookeeper.client.ZkClient
import com.twitter.finagle.exp.zookeeper.watch.WatchManager
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util._
/*

class SessionMar(
  richClient: ZkClient,
  watchManagr: WatchManager,
  autoReconnection: Boolean = true) {

  @volatile private[this] var session: Session = null
  /**
   * State variables
   * state - describes the current state of the connection
   * isFirstConnect - if it's the first connection
   * autoReconnect - if you want to reconnect in case of connection loss
   */

  private[this] var autoReconnect: Boolean = autoReconnection

  /**
   * Local variables
   * writer - use to write requests to the transport
   * watchManager - use to get watches in case of reconnection
   * pingScheduler - use to send ping request every x milliseconds
   */
  private[this] val watchManager = watchManagr




  /**
   * Classic way to check a response header ( error field )
   * This will first parse the header, the state will be changed if required
   * then checkState will verify if the connection is still alive
   * @param header a response header
   */
  def apply(header: ReplyHeader) {
    checkHeader(header)
    checkState()
  }

  /**
   * Classic way to check a watch event ( state field )
   * This will first parse the WatchEvent, the state will be changed if required
   * then checkState will verify if the connection is still alive
   * @param event a watch event
   */
  def apply(event: WatchEvent) {
    checkWatchEvent(event)
    checkState()
  }

  /**
   * Connection management :
   * -- prepareConnection
   * It is called when the connectRequest is written in the Transport
   *
   * -- completeConnection
   * Called if the connection succeeded (after connectResponse decoding)
   *
   * -- connectionFailed
   * Called if the connection is not established
   */
  def prepareConnection() { state = States.CONNECTING }

  def completeConnection(response: ConnectResponse) {
    if (isFirstConnect) isFirstConnect = false
    sessionId = response.sessionId
    sessionTimeOut = response.timeOut
    sessionPassword = response.passwd
    state = States.CONNECTED
    startPing(richClient.ping())
  }

  def connectionFailed() {
    state = States.CLOSED
  }



  def isClosing: Boolean = {
    isClosingSession.get()
  }



  /**
   * Before connection, we check if we are already trying to connect, or
   * if the connection is already established
   */
  def checkStateBeforeConnect() {
    if (!isFirstConnect && state == States.CONNECTED || isFirstConnect && state == States.CONNECTED)
      throw new RuntimeException("You are already connected ! don't try to connect")
    else if (isFirstConnect && state == States.CONNECTING)
      throw new RuntimeException("Connection in progress ! don't try to connect")
  }



  /**
   * Here we are parsing the watchEvent's state field
   * and changing the connection state if required
   * @param event a request header
   */
  def checkWatchEvent(event: WatchEvent) {
    event.state match {
      case 0 => state = States.NOT_CONNECTED
      case 3 => state = States.CONNECTED
      case -112 => state = States.SESSION_EXPIRED
      case _ =>
    }
  }

  /**
   * We try to reconnect with the last session informations,
   * if the connection can't be restablished (maybe session has expired)
   * then a new connection is tried.
   * @return Future[Unit]
   */
  def reconnect: Future[Unit] = {
    val recoReq = new ConnectRequest(0, lastZxid, sessionTimeOut, sessionId, sessionPassword)
    prepareConnection()
    Try { writer(recoReq) } match {
      case Return(rep) =>
        rep flatMap { connectResponse =>
          completeConnection(connectResponse.asInstanceOf[ConnectResponse])
          // TODO do we need to set watches back if the session has not expired
          Future(Unit)
        }
      case Throw(exc) =>
        // New connection
        connect.unit
    }
  }

  /**
   * We try to create a new session
   * @return Future[Unit]
   */
  def connect: Future[Unit] = {
    val recoReq = new ConnectRequest(connectionTimeout = sessionTimeOut)
    prepareConnection()
    Try { writer(recoReq) } match {
      case Return(rep) =>
        rep flatMap { connectResponse =>
          // TODO set watches back if it's a reconnection
          completeConnection(connectResponse.asInstanceOf[ConnectResponse])
          Future(Unit)
        }
      case Throw(exc) =>
        state = States.CLOSED
        throw exc
    }
  }
}

*/
