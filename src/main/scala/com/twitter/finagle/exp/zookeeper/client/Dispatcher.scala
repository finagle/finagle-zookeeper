package com.twitter.finagle.exp.zookeeper.client

import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{Failure, Service}
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.util._
import com.twitter.concurrent.AsyncSemaphore
import com.twitter.finagle.exp.zookeeper._
import com.twitter.util.Throw
import java.net.InetSocketAddress


abstract class BasicDispatcher(trans: Transport[ChannelBuffer, ChannelBuffer])
  extends Service[Request, Response] {

  private[this] val semaphore = new AsyncSemaphore(1)
  private[this] val localAddress: InetSocketAddress = trans.localAddress match {
    case ia: InetSocketAddress => ia
    case _ => new InetSocketAddress(0)
  }
  override def isAvailable = trans.isOpen
  override def close(deadline: Time) = trans.close()

  protected def dispatch(req: Request, p: Promise[Response]): Future[Unit]

  private[this] def tryDispatch(req: Request, p: Promise[Response]): Future[Unit] =
    p.isInterrupted match {
      case Some(intr) =>
        p.setException(Failure.InterruptedBy(intr))
        Future.Done
      case None =>
        p.setInterruptHandler { case intr =>
          if (p.updateIfEmpty(Throw(intr)))
            trans.close()
        }

        dispatch(req, p)
    }

  def apply(request: Request): Future[Response] = {
    val p = new Promise[Response]

    semaphore.acquire() onSuccess { permit =>
      tryDispatch(request, p) onFailure { exc =>
        p.updateIfEmpty(Throw(exc))
      } ensure {
        permit.release()
      }
    } onFailure { p.setException }
    p
  }
}

class ZkDispatcher(trans: Transport[ChannelBuffer, ChannelBuffer]) extends BasicDispatcher(trans) {
  //we give the processor apply, thus it can send/read Req-Rep cf: Ping
  val requestMatcher = new RequestMatcher(trans, apply)

  protected def dispatch(req: Request, p: Promise[Response]): Future[Unit] = {
    requestMatcher.write(req) respond { resp =>
      p.updateIfEmpty(resp)
    }
  }.unit
}



