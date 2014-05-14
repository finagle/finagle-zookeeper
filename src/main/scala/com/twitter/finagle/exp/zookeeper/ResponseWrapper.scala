package com.twitter.finagle.exp.zookeeper

import com.twitter.finagle.exp.zookeeper.ZookeeperDefinitions.opCode._
import com.twitter.util.Try

object ResponseWrapper {
  /* Decode a BufferedResponse to type Try[T] by pattern matching opCode (XID) */
  def decode[T >: Response](repBuffer: BufferedResponse, opCode: Int): Try[T] = opCode match {
    case `createSession` => ConnectResponse(repBuffer.buffer)
    case `ping` => ReplyHeader(repBuffer.buffer)
    case `closeSession` => ReplyHeader(repBuffer.buffer)
    case `create` => CreateResponse(repBuffer.buffer)
    case `delete` => ReplyHeader(repBuffer.buffer)
    case `exists` => ExistsResponse(repBuffer.buffer)
    case `getACL` => GetACLResponse(repBuffer.buffer)
    case `getChildren` => GetChildrenResponse(repBuffer.buffer)
    case `getChildren2` => GetChildren2Response(repBuffer.buffer)
    case `getData` => GetDataResponse(repBuffer.buffer)
    case `setData` => SetDataResponse(repBuffer.buffer)
    case `setACL` => SetACLResponse(repBuffer.buffer)
    case `sync` => SyncResponse(repBuffer.buffer)
    case `setWatches` => ReplyHeader(repBuffer.buffer)
  }
}
