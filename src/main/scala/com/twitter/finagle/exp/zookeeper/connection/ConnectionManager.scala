package com.twitter.finagle.exp.zookeeper.connection

import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.exp.zookeeper._

/**
 * The connection manager is supposed to handle the connection
 * whether it is in standalone or quorum mode
 */
class ConnectionManager(dest: String) {

  @volatile var connection: Connection = new Connection(findNextServer)

  def findNextServer: ServiceFactory[ReqPacket, RepPacket] = {
    val serviceFactory = ZooKeeperClient.newClient(formatHostList(dest)(0))
    serviceFactory
  }

  def formatHostList(list: String): Seq[String] = list.trim.split(",")
}
