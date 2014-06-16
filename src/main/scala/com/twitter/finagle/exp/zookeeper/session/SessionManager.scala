package com.twitter.finagle.exp.zookeeper.session

import com.twitter.finagle.exp.zookeeper.ConnectResponse
import com.twitter.finagle.exp.zookeeper.client.ZkClient
import com.twitter.finagle.exp.zookeeper.session.Session.States

class SessionManager(autoReconnection: Boolean = true, richClient: ZkClient) {

  @volatile var session: Session = new Session()

  def initSession(conReq: ConnectResponse) {
    session = new Session(
      conReq.sessionId,
      conReq.passwd,
      conReq.timeOut)
    session.state = States.CONNECTED
    session.isFirstConnect = false
  }
}


