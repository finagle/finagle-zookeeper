package com.twitter.finagle.exp.zookeeper.integration

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class LocalSessionUpdate extends FunSuite with IntegrationConfig {
  test("updates local session") {
    /*
    private void testLocalSessionUpgrade(boolean testLeader) throws Exception {

         int leaderIdx = qb.getLeaderIndex();
         Assert.assertFalse("No leader in quorum?", leaderIdx == -1);
         int followerIdx = (leaderIdx + 1) % 5;
         int testPeerIdx = testLeader ? leaderIdx : followerIdx;
         String hostPorts[] = qb.hostPort.split(",");

         CountdownWatcher watcher = new CountdownWatcher();
         ZooKeeper zk = qb.createClient(watcher, hostPorts[testPeerIdx],
                 CONNECTION_TIMEOUT);
         watcher.waitForConnected(CONNECTION_TIMEOUT);

         final String firstPath = "/first";
         final String secondPath = "/ephemeral";

         // Just create some node so that we know the current zxid
         zk.create(firstPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                 CreateMode.PERSISTENT);

         // Now, try an ephemeral node. This will trigger session upgrade
         // so there will be createSession request inject into the pipeline
         // prior to this request
         zk.create(secondPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                 CreateMode.EPHEMERAL);

         Stat firstStat = zk.exists(firstPath, null);
         Assert.assertNotNull(firstStat);

         Stat secondStat = zk.exists(secondPath, null);
         Assert.assertNotNull(secondStat);

         long zxidDiff = secondStat.getCzxid() - firstStat.getCzxid();

         // If there is only one createSession request in between, zxid diff
         // will be exactly 2. The alternative way of checking is to actually
         // read txnlog but this should be sufficient
         Assert.assertEquals(2L, zxidDiff);

     }
     */
  }
}
