package com.twitter.finagle.exp.zookeeper.unit.session

import java.util.concurrent.atomic.AtomicBoolean

import com.twitter.finagle.exp.zookeeper.ConnectResponse
import com.twitter.finagle.exp.zookeeper.session.Session
import com.twitter.finagle.exp.zookeeper.session.Session.States
import com.twitter.util.TimeConversions._
import com.twitter.util.{Future, Throw}
import org.scalatest.FunSuite

class SessionTest extends FunSuite {
  test("should init correctly") {
    val session = new Session(
      sessionID = 12315641L,
      sessionTimeout = 3000.milliseconds,
      negotiateTimeout = 2000.milliseconds,
      pingSender = Some(() => Future.Done)
    )
    session.init()
    assert(session.PingScheduler.isRunning)
    assert(session.nextXid === 2)
    assert(session.nextXid === 3)
    assert(session.lastZxid.get() === 0L)
    assert(session.isReadOnly === false)
    assert(session.isClosingSession.get() === false)
    assert(session.currentState.get() === States.CONNECTED)
    assert(session.hasFakeSessionId.get() === false)

    val session2 = new Session(
      sessionID = 12315641L,
      sessionTimeout = 3000.milliseconds,
      negotiateTimeout = 2000.milliseconds,
      isRO = new AtomicBoolean(true),
      pingSender = Some(() => Future.Done)
    )
    session2.init()
    assert(session2.PingScheduler.isRunning)
    assert(session2.nextXid === 2)
    assert(session2.nextXid === 3)
    assert(session2.lastZxid.get() === 0L)
    assert(session2.isReadOnly === true)
    assert(session2.isClosingSession.get() === false)
    assert(session2.currentState.get() === States.CONNECTED_READONLY)
    assert(session2.hasFakeSessionId.get() === true)
  }

  test("should not init") {
    val session = new Session()
    intercept[RuntimeException] {
      session.init()
    }
    val session2 = new Session(
      sessionID = 12315641L,
      sessionTimeout = 3000.milliseconds,
      negotiateTimeout = 2000.milliseconds,
      pingSender = Some(() => Future.Done)
    )
    session2.init()
    intercept[RuntimeException] {
      session.init()
    }
  }

  test("should reinit the session") {
    val session = new Session(
      sessionID = 12315641L,
      sessionTimeout = 3000.milliseconds,
      negotiateTimeout = 2000.milliseconds,
      pingSender = Some(() => Future.Done)
    )
    session.init()
    assert(session.PingScheduler.isRunning)
    assert(session.nextXid === 2)
    assert(session.nextXid === 3)
    assert(session.lastZxid.get() === 0L)
    assert(session.isReadOnly === false)
    assert(session.isClosingSession.get() === false)
    assert(session.currentState.get() === States.CONNECTED)
    assert(session.hasFakeSessionId.get() === false)
    val conRep = ConnectResponse(
      0,
      2000.milliseconds,
      12315641L,
      Array[Byte](16),
      true
    )
    session.reinit(conRep, () => Future.Done)
    assert(session.PingScheduler.isRunning)
    assert(session.nextXid === 2)
    assert(session.nextXid === 3)
    assert(session.lastZxid.get() === 0L)
    assert(session.isReadOnly === true)
    assert(session.isClosingSession.get() === false)
    assert(session.currentState.get() === States.CONNECTED_READONLY)
    assert(session.hasFakeSessionId.get() === false)

    val session2 = new Session(
      sessionID = 12315641L,
      sessionTimeout = 3000.milliseconds,
      negotiateTimeout = 2000.milliseconds,
      isRO = new AtomicBoolean(true),
      pingSender = Some(() => Future.Done)
    )
    session2.init()
    assert(session2.PingScheduler.isRunning)
    assert(session2.nextXid === 2)
    assert(session2.nextXid === 3)
    assert(session2.lastZxid.get() === 0L)
    assert(session2.isClosingSession.get() === false)
    assert(session2.isReadOnly === true)
    assert(session2.currentState.get() === States.CONNECTED_READONLY)
    assert(session2.hasFakeSessionId.get() === true)

    val conRep2 = ConnectResponse(
      0,
      2000.milliseconds,
      12315641L,
      Array[Byte](16),
      isRO = false
    )
    session2.reinit(conRep2, () => Future.Done)
    assert(session2.PingScheduler.isRunning)
    assert(session2.nextXid === 2)
    assert(session2.nextXid === 3)
    assert(session2.lastZxid.get() === 0L)
    assert(session2.isReadOnly === false)
    assert(session2.isClosingSession.get() === false)
    assert(session2.currentState.get() === States.CONNECTED)
    assert(session2.hasFakeSessionId.get() === false)
  }

  test("should not reinit the session") {
    val session = new Session(
      sessionID = 12315641L,
      sessionTimeout = 3000.milliseconds,
      negotiateTimeout = 2000.milliseconds,
      pingSender = Some(() => Future.Done)
    )
    session.init()
    val conRep = ConnectResponse(
      0,
      2000.milliseconds,
      1782315641L,
      Array[Byte](16),
      true
    )
    session.reinit(conRep, () => Future.Done) match {
      case Throw(exc) =>
        assert(exc.isInstanceOf[AssertionError])
      case _ =>
    }
  }

  test("should close correctly") {
    val session = new Session(
      sessionID = 12315641L,
      sessionTimeout = 3000.milliseconds,
      negotiateTimeout = 2000.milliseconds,
      pingSender = Some(() => Future.Done)
    )
    session.init()
    session.close()
    assert(session.isClosingSession.get() === false)
    assert(session.currentState.get() === States.CLOSED)
  }

  test("should prepareClose correctly") {
    val session = new Session(
      sessionID = 12315641L,
      sessionTimeout = 3000.milliseconds,
      negotiateTimeout = 2000.milliseconds,
      pingSender = Some(() => Future.Done)
    )
    session.init()
    session.prepareClose()
    assert(!session.PingScheduler.isRunning)
    assert(session.isClosingSession.get() === true)
  }
}