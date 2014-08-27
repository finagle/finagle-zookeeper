package com.twitter.finagle.exp.zookeeper.unit.session

import java.util.concurrent.atomic.AtomicBoolean

import com.twitter.finagle.exp.zookeeper.watcher.Watch
import com.twitter.finagle.exp.zookeeper.{WatchEvent, ReplyHeader, ConnectResponse}
import com.twitter.finagle.exp.zookeeper.session.Session.States
import com.twitter.finagle.exp.zookeeper.session.{Session, SessionManager}
import com.twitter.util.Future
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import com.twitter.util.TimeConversions._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SessionManagerTest extends FunSuite {
  test("should build a good connectRequest") {
    val sessionManager = new SessionManager(true)
    val conReq = sessionManager.buildConnectRequest(3000.milliseconds)
    assert(conReq.lastZxidSeen === 0l)
    assert(conReq.protocolVersion === 0)
    assert(conReq.sessionTimeout === 3000.milliseconds)
    assert(conReq.canBeRO === true)
  }

  test("should build a good reconnectRequest with a fake session") {
    val sessionManager = new SessionManager(true)
    sessionManager.session.hasFakeSessionId.set(true)
    sessionManager.session.lastZxid.set(123556L)
    val conReq = sessionManager.buildReconnectRequest(Some(3000.milliseconds))
    assert(conReq.lastZxidSeen === 123556L)
    assert(conReq.protocolVersion === 0)
    assert(conReq.sessionId === 0L)
    assert(conReq.sessionTimeout === 3000.milliseconds)
    assert(conReq.canBeRO === true)
  }

  test("should build a good reconnectRequest with a real session") {
    val sessionManager = new SessionManager(true)
    sessionManager.session = new Session(
      sessionID = 12315641L,
      sessionTimeout = 3000.milliseconds,
      negotiateTimeout = 2000.milliseconds,
      pingSender = Some(() => Future(println("ping")))
    )
    sessionManager.session.hasFakeSessionId.set(false)
    val conReq = sessionManager.buildReconnectRequest(Some(3000.milliseconds))
    assert(conReq.lastZxidSeen === 0L)
    assert(conReq.protocolVersion === 0)
    assert(conReq.sessionId === 12315641L)
    assert(conReq.sessionTimeout === 3000.milliseconds)
    assert(conReq.canBeRO === true)
  }

  test("should create a new session correctly") {
    val sessionManager = new SessionManager(true)
    val conRep = ConnectResponse(
      0,
      2000.milliseconds,
      12315641L,
      Array[Byte](16),
      true
    )
    sessionManager.newSession(conRep, 3000.milliseconds, () => Future.Done)
    assert(sessionManager.session.PingScheduler.isRunning)
    assert(sessionManager.session.nextXid === 2)
    assert(sessionManager.session.nextXid === 3)
    assert(sessionManager.session.lastZxid.get() === 0L)
    assert(sessionManager.session.isReadOnly === true)
    assert(sessionManager.session.hasSessionClosed.get() === false)
    assert(sessionManager.session.currentState.get() === States.CONNECTED_READONLY)
    assert(sessionManager.session.hasFakeSessionId.get() === true)
  }

  test("should parse a header") {
    val header = ReplyHeader(12, 124, -4)
    val sessionManager = new SessionManager(true)
    sessionManager.parseHeader(header)
    assert(!sessionManager.session.PingScheduler.isRunning)
    assert(sessionManager.session.currentState.get() === States.CONNECTION_LOSS)

    val header2 = ReplyHeader(12, 124, -112)
    val sessionManager2 = new SessionManager(true)
    sessionManager2.parseHeader(header2)
    assert(!sessionManager2.session.PingScheduler.isRunning)
    assert(sessionManager2.session.currentState.get() === States.SESSION_EXPIRED)

    val header3 = ReplyHeader(12, 124, -115)
    val sessionManager3 = new SessionManager(true)
    sessionManager3.parseHeader(header3)
    assert(sessionManager3.session.currentState.get() === States.AUTH_FAILED)

    val header4 = ReplyHeader(12, 124, -118)
    val sessionManager4 = new SessionManager(true)
    sessionManager4.parseHeader(header4)
    assert(!sessionManager4.session.PingScheduler.isRunning)
    assert(sessionManager4.session.currentState.get() === States.SESSION_MOVED)
  }

  test("should parse a WatchEvent") {
    val watchEvent = WatchEvent(Watch.EventType.NONE, Watch.EventState.EXPIRED, "")
    val sessionManager = new SessionManager(true)
    sessionManager.parseWatchEvent(watchEvent)
    assert(!sessionManager.session.PingScheduler.isRunning)
    assert(sessionManager.session.currentState.get() === States.SESSION_EXPIRED)

    val watchEvent2 = WatchEvent(Watch.EventType.NONE, Watch.EventState.DISCONNECTED, "")
    val sessionManager2 = new SessionManager(true)
    sessionManager2.parseWatchEvent(watchEvent2)
    assert(!sessionManager2.session.PingScheduler.isRunning)
    assert(sessionManager2.session.currentState.get() === States.NOT_CONNECTED)


    val watchEvent3 = WatchEvent(Watch.EventType.NONE, Watch.EventState.SYNC_CONNECTED, "")
    val sessionManager3 = new SessionManager(true)
    sessionManager3.parseWatchEvent(watchEvent3)
    assert(sessionManager3.session.currentState.get() === States.CONNECTED)


    val watchEvent4 = WatchEvent(Watch.EventType.NONE, Watch.EventState.CONNECTED_READ_ONLY, "")
    val sessionManager4 = new SessionManager(true)
    sessionManager4.parseWatchEvent(watchEvent4)
    assert(sessionManager4.session.currentState.get() === States.CONNECTED_READONLY)
  }

  test("should reinit") {
    val sessionManager = new SessionManager(true)
    sessionManager.session = new Session(
      sessionID = 12315641L,
      sessionTimeout = 3000.milliseconds,
      negotiateTimeout = 2000.milliseconds,
      isRO = new AtomicBoolean(true),
      pingSender = Some(() => Future.Done)
    )
    sessionManager.session.init()
    assert(sessionManager.session.PingScheduler.isRunning)
    assert(sessionManager.session.nextXid === 2)
    assert(sessionManager.session.nextXid === 3)
    assert(sessionManager.session.lastZxid.get() === 0L)
    assert(sessionManager.session.hasSessionClosed.get() === false)
    assert(sessionManager.session.isReadOnly === true)
    assert(sessionManager.session.currentState.get() === States.CONNECTED_READONLY)
    assert(sessionManager.session.hasFakeSessionId.get() === true)

    val conRep2 = ConnectResponse(
      0,
      2000.milliseconds,
      12315641L,
      Array[Byte](16),
      isRO = false
    )
    sessionManager.session.reinit(conRep2, () => Future.Done)
    assert(sessionManager.session.PingScheduler.isRunning)
    assert(sessionManager.session.nextXid === 2)
    assert(sessionManager.session.nextXid === 3)
    assert(sessionManager.session.lastZxid.get() === 0L)
    assert(sessionManager.session.isReadOnly === false)
    assert(sessionManager.session.hasSessionClosed.get() === false)
    assert(sessionManager.session.currentState.get() === States.CONNECTED)
    assert(sessionManager.session.hasFakeSessionId.get() === false)
  }
}