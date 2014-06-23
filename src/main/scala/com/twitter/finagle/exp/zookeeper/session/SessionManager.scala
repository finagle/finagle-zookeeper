package com.twitter.finagle.exp.zookeeper.session

import com.twitter.finagle.exp.zookeeper.ConnectResponse
import com.twitter.finagle.exp.zookeeper.client.ZkClient
import com.twitter.finagle.exp.zookeeper.session.Session.States
import com.twitter.util.Duration

class SessionManager(
  autoReconnection: Boolean = true,
  autoWatchReset: Boolean = true,
  richClient: ZkClient) {

  @volatile var session: Session = new Session()
  var hasConnectedRwServer = false
  @volatile var isSearchingRwServer = false

  def initSession(conReq: ConnectResponse, sessionTimeout: Duration) {
    session = new Session(
      conReq.sessionId,
      conReq.passwd,
      sessionTimeout,
      conReq.timeOut,
      conReq.isRO)
    session.state = States.CONNECTED
    session.isFirstConnect.set(false)
  }
}


