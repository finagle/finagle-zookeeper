package com.twitter.finagle.exp.zookeeper.integration

import com.twitter.finagle.exp.zookeeper.NoAuthException
import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.CreateMode
import com.twitter.finagle.exp.zookeeper.data.{Auth, Ids}
import com.twitter.util.Await
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RichClientTest extends FunSuite with IntegrationConfig {
  test("async test") {
    newClient()
    connect()

    intercept[NoAuthException] {
      Await.result {
        client.get.addAuth(Auth("digest", "ben:passwd".getBytes)) before
          client.get.create("/ben", "".getBytes, Ids.READ_ACL_UNSAFE, CreateMode.PERSISTENT).unit before
          client.get.create("/ben/2", "".getBytes, Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT)
      }
    }

    Await.result {
      for {
        _ <- client.get.delete("/ben", -1)
        _ <- client.get.create("/ben2", "".getBytes, Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT)
        _ <- client.get.getData("/ben2")
      } yield None
    }

    disconnect()
    Await.ready(client.get.closeService())
    newClient()
    connect()

    intercept[NoAuthException] {
      Await.result {
        client.get.addAuth(Auth("digest", "ben:passwd2".getBytes)) before
          client.get.getData("/ben2")
      }
    }

    disconnect()
    Await.ready(client.get.closeService())
    newClient()
    connect()

    Await.result {
      client.get.addAuth(Auth("digest", "ben:passwd".getBytes)) before
        client.get.getData("/ben2")
    }

    Await.result {
      client.get.delete("/ben2", -1)
    }

    disconnect()
    Await.ready(client.get.closeService())
  }

  test("rich client test") {
    /*
     ZooKeeper zk = null;
        try {
            MyWatcher watcher = new MyWatcher();
            zk = createClient(watcher, hostPort);
            LOG.info("Before create /benwashere");
            zk.create("/benwashere", "".getBytes(), Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            LOG.info("After create /benwashere");
            try {
                zk.setData("/benwashere", "hi".getBytes(), 57);
                Assert.fail("Should have gotten BadVersion exception");
            } catch(KeeperException.BadVersionException e) {
                // expected that
            } catch (KeeperException e) {
                Assert.fail("Should have gotten BadVersion exception");
            }
            LOG.info("Before delete /benwashere");
            zk.delete("/benwashere", 0);
            LOG.info("After delete /benwashere");
            zk.close();
            //LOG.info("Closed client: " + zk.describeCNXN());
            Thread.sleep(2000);

            zk = createClient(watcher, hostPort);
            //LOG.info("Created a new client: " + zk.describeCNXN());
            LOG.info("Before delete /");

            try {
                zk.delete("/", -1);
                Assert.fail("deleted root!");
            } catch(KeeperException.BadArgumentsException e) {
                // good, expected that
            }
            Stat stat = new Stat();
            // Test basic create, ls, and getData
            zk.create("/pat", "Pat was here".getBytes(), Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            LOG.info("Before create /ben");
            zk.create("/pat/ben", "Ben was here".getBytes(),
                    Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            LOG.info("Before getChildren /pat");
            List<String> children = zk.getChildren("/pat", false);
            Assert.assertEquals(1, children.size());
            Assert.assertEquals("ben", children.get(0));
            List<String> children2 = zk.getChildren("/pat", false, null);
            Assert.assertEquals(children, children2);
            String value = new String(zk.getData("/pat/ben", false, stat));
            Assert.assertEquals("Ben was here", value);
            // Test stat and watch of non existent node

            try {
                if (withWatcherObj) {
                    Assert.assertEquals(null, zk.exists("/frog", watcher));
                } else {
                    Assert.assertEquals(null, zk.exists("/frog", true));
                }
                LOG.info("Comment: asseting passed for frog setting /");
            } catch (KeeperException.NoNodeException e) {
                // OK, expected that
            }
            zk.create("/frog", "hi".getBytes(), Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            // the first poll is just a session delivery
            LOG.info("Comment: checking for events length "
                     + watcher.events.size());
            WatchedEvent event = watcher.events.poll(10, TimeUnit.SECONDS);
            Assert.assertEquals("/frog", event.getPath());
            Assert.assertEquals(EventType.NodeCreated, event.getType());
            Assert.assertEquals(KeeperState.SyncConnected, event.getState());
            // Test child watch and create with sequence
            zk.getChildren("/pat/ben", true);
            for (int i = 0; i < 10; i++) {
                zk.create("/pat/ben/" + i + "-", Integer.toString(i).getBytes(),
                        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            }
            children = zk.getChildren("/pat/ben", false);
            Collections.sort(children);
            Assert.assertEquals(10, children.size());
            for (int i = 0; i < 10; i++) {
                final String name = children.get(i);
                Assert.assertTrue("starts with -", name.startsWith(i + "-"));
                byte b[];
                if (withWatcherObj) {
                    b = zk.getData("/pat/ben/" + name, watcher, stat);
                } else {
                    b = zk.getData("/pat/ben/" + name, true, stat);
                }
                Assert.assertEquals(Integer.toString(i), new String(b));
                zk.setData("/pat/ben/" + name, "new".getBytes(),
                        stat.getVersion());
                if (withWatcherObj) {
                    stat = zk.exists("/pat/ben/" + name, watcher);
                } else {
                stat = zk.exists("/pat/ben/" + name, true);
                }
                zk.delete("/pat/ben/" + name, stat.getVersion());
            }
            event = watcher.events.poll(10, TimeUnit.SECONDS);
            Assert.assertEquals("/pat/ben", event.getPath());
            Assert.assertEquals(EventType.NodeChildrenChanged, event.getType());
            Assert.assertEquals(KeeperState.SyncConnected, event.getState());
            for (int i = 0; i < 10; i++) {
                event = watcher.events.poll(10, TimeUnit.SECONDS);
                final String name = children.get(i);
                Assert.assertEquals("/pat/ben/" + name, event.getPath());
                Assert.assertEquals(EventType.NodeDataChanged, event.getType());
                Assert.assertEquals(KeeperState.SyncConnected, event.getState());
                event = watcher.events.poll(10, TimeUnit.SECONDS);
                Assert.assertEquals("/pat/ben/" + name, event.getPath());
                Assert.assertEquals(EventType.NodeDeleted, event.getType());
                Assert.assertEquals(KeeperState.SyncConnected, event.getState());
            }
            zk.create("/good\u0040path", "".getBytes(), Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);

            zk.create("/duplicate", "".getBytes(), Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            try {
                zk.create("/duplicate", "".getBytes(), Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
                Assert.fail("duplicate create allowed");
            } catch(KeeperException.NodeExistsException e) {
                // OK, expected that
            }
        } finally {
            if (zk != null) {
                zk.close();
            }
        }
     */
  }

  test("sequential nodes") {
    /*
    public void testSequentialNodeNames()
        throws IOException, InterruptedException, KeeperException
    {
        String path = "/SEQUENCE";
        String file = "TEST";
        String filepath = path + "/" + file;

        ZooKeeper zk = null;
        try {
            zk = createClient();
            zk.create(path, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create(filepath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            List<String> children = zk.getChildren(path, false);
            Assert.assertEquals(1, children.size());
            Assert.assertEquals(file + "0000000000", children.get(0));

            zk.create(filepath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            children = zk.getChildren(path, false);
            Assert.assertEquals(2, children.size());
            Assert.assertTrue("contains child 1",  children.contains(file + "0000000001"));

            zk.create(filepath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            children = zk.getChildren(path, false);
            Assert.assertEquals(3, children.size());
            Assert.assertTrue("contains child 2",
                       children.contains(file + "0000000002"));

            // The pattern is holding so far.  Let's run the counter a bit
            // to be sure it continues to spit out the correct answer
            for(int i = children.size(); i < 105; i++)
               zk.create(filepath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

            children = zk.getChildren(path, false);
            Assert.assertTrue("contains child 104",
                       children.contains(file + "0000000104"));

        }
        finally {
            if(zk != null)
                zk.close();
        }
     */
  }

  test("sequential nodes with data") {
    /*
    public void testSequentialNodeData() throws Exception {
        ZooKeeper zk= null;
        String queue_handle = "/queue";
        try {
            zk = createClient();

            zk.create(queue_handle, new byte[0], Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            zk.create(queue_handle + "/element", "0".getBytes(),
                    Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            zk.create(queue_handle + "/element", "1".getBytes(),
                    Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            List<String> children = zk.getChildren(queue_handle, true);
            Assert.assertEquals(children.size(), 2);
            String child1 = children.get(0);
            String child2 = children.get(1);
            int compareResult = child1.compareTo(child2);
            Assert.assertNotSame(compareResult, 0);
            if (compareResult < 0) {
            } else {
                String temp = child1;
                child1 = child2;
                child2 = temp;
            }
            String child1data = new String(zk.getData(queue_handle
                    + "/" + child1, false, null));
            String child2data = new String(zk.getData(queue_handle
                    + "/" + child2, false, null));
            Assert.assertEquals(child1data, "0");
            Assert.assertEquals(child2data, "1");
        } finally {
            if (zk != null) {
                zk.close();
            }
        }

    }
     */
  }

  test("large node") {
    /*
    public void testLargeNodeData() throws Exception {
        ZooKeeper zk= null;
        String queue_handle = "/large";
        try {
            zk = createClient();

            zk.create(queue_handle, new byte[500000], Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        } finally {
            if (zk != null) {
                zk.close();
            }
        }

    }
     */
  }

  test("create fails") {
    /*
    private void verifyCreateFails(String path, ZooKeeper zk) throws Exception {
        try {
            zk.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (IllegalArgumentException e) {
            // this is good
            return;
        }
        Assert.fail("bad path \"" + path + "\" not caught");
    }

    // Test that the path string is validated
    @Test
    public void testPathValidation() throws Exception {
        ZooKeeper zk = createClient();

        verifyCreateFails(null, zk);
        verifyCreateFails("", zk);
        verifyCreateFails("//", zk);
        verifyCreateFails("///", zk);
        verifyCreateFails("////", zk);
        verifyCreateFails("/.", zk);
        verifyCreateFails("/..", zk);
        verifyCreateFails("/./", zk);
        verifyCreateFails("/../", zk);
        verifyCreateFails("/foo/./", zk);
        verifyCreateFails("/foo/../", zk);
        verifyCreateFails("/foo/.", zk);
        verifyCreateFails("/foo/..", zk);
        verifyCreateFails("/./.", zk);
        verifyCreateFails("/../..", zk);
        verifyCreateFails("/\u0001foo", zk);
        verifyCreateFails("/foo/bar/", zk);
        verifyCreateFails("/foo//bar", zk);
        verifyCreateFails("/foo/bar//", zk);

        verifyCreateFails("foo", zk);
        verifyCreateFails("a", zk);

        zk.create("/createseqpar", null, Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        // next two steps - related to sequential processing
        // 1) verify that empty child name Assert.fails if not sequential
        try {
            zk.create("/createseqpar/", null, Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            Assert.assertTrue(false);
        } catch(IllegalArgumentException be) {
            // catch this.
        }

        // 2) verify that empty child name success if sequential
        zk.create("/createseqpar/", null, Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_SEQUENTIAL);
        zk.create("/createseqpar/.", null, Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_SEQUENTIAL);
        zk.create("/createseqpar/..", null, Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_SEQUENTIAL);
        try {
            zk.create("/createseqpar//", null, Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT_SEQUENTIAL);
            Assert.assertTrue(false);
        } catch(IllegalArgumentException be) {
            // catch this.
        }
        try {
            zk.create("/createseqpar/./", null, Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT_SEQUENTIAL);
            Assert.assertTrue(false);
        } catch(IllegalArgumentException be) {
            // catch this.
        }
        try {
            zk.create("/createseqpar/../", null, Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT_SEQUENTIAL);
            Assert.assertTrue(false);
        } catch(IllegalArgumentException be) {
            // catch this.
        }


        //check for the code path that throws at server
        PrepRequestProcessor.setFailCreate(true);
        try {
            zk.create("/m", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            Assert.assertTrue(false);
        } catch(KeeperException.BadArgumentsException be) {
            // catch this.
        }
        PrepRequestProcessor.setFailCreate(false);
        zk.create("/.foo", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/.f.", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/..f", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/..f..", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/f.c", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/f\u0040f", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/f", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/f/.f", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/f/f.", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/f/..f", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/f/f..", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/f/.f/f", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/f/f./f", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
     */
  }

  test("delete with children") {
    /*
    public void testDeleteWithChildren() throws Exception {
        ZooKeeper zk = createClient();
        zk.create("/parent", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/parent/child", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        try {
            zk.delete("/parent", -1);
            Assert.fail("Should have received a not equals message");
        } catch (KeeperException e) {
            Assert.assertEquals(KeeperException.Code.NOTEMPTY, e.code());
        }
        zk.delete("/parent/child", -1);
        zk.delete("/parent", -1);
        zk.close();
    }

    private class VerifyClientCleanup extends Thread {
        int count;
        int current = 0;

        VerifyClientCleanup(String name, int count) {
            super(name);
            this.count = count;
        }

        public void run() {
            try {
                for (; current < count; current++) {
                    TestableZooKeeper zk = createClient();
                    zk.close();
                    // we've asked to close, wait for it to finish closing
                    // all the sub-threads otw the selector may not be
                    // closed when we check (false positive on test Assert.failure
                    zk.testableWaitForShutdown(CONNECTION_TIMEOUT);
                }
            } catch (Throwable t) {
                LOG.error("test Assert.failed", t);
            }
        }
    }
     */
  }

  test("getChildren test 1") {
    /*
public void testChild()
        throws IOException, KeeperException, InterruptedException
    {
        String name = "/foo";
        zk.create(name, name.getBytes(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        String childname = name + "/bar";
        zk.create(childname, childname.getBytes(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);

        Stat stat = new Stat();
        List<String> s = zk.getChildren(name, false, stat);

        Assert.assertEquals(stat.getCzxid(), stat.getMzxid());
        Assert.assertEquals(stat.getCzxid() + 1, stat.getPzxid());
        Assert.assertEquals(stat.getCtime(), stat.getMtime());
        Assert.assertEquals(1, stat.getCversion());
        Assert.assertEquals(0, stat.getVersion());
        Assert.assertEquals(0, stat.getAversion());
        Assert.assertEquals(0, stat.getEphemeralOwner());
        Assert.assertEquals(name.length(), stat.getDataLength());
        Assert.assertEquals(1, stat.getNumChildren());
        Assert.assertEquals(s.size(), stat.getNumChildren());

        s = zk.getChildren(childname, false, stat);

        Assert.assertEquals(stat.getCzxid(), stat.getMzxid());
        Assert.assertEquals(stat.getCzxid(), stat.getPzxid());
        Assert.assertEquals(stat.getCtime(), stat.getMtime());
        Assert.assertEquals(0, stat.getCversion());
        Assert.assertEquals(0, stat.getVersion());
        Assert.assertEquals(0, stat.getAversion());
        Assert.assertEquals(zk.getSessionId(), stat.getEphemeralOwner());
        Assert.assertEquals(childname.length(), stat.getDataLength());
        Assert.assertEquals(0, stat.getNumChildren());
        Assert.assertEquals(s.size(), stat.getNumChildren());
    }
     */
  }

  test("getChildren test 2") {
    /*
public void testChildren()
        throws IOException, KeeperException, InterruptedException
    {
        String name = "/foo";
        zk.create(name, name.getBytes(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        List<String> children = new ArrayList<String>();
        List<String> children_s = new ArrayList<String>();

        for (int i = 0; i < 10; i++) {
            String childname = name + "/bar" + i;
            String childname_s = "bar" + i;
            children.add(childname);
            children_s.add(childname_s);
        }

        for(int i = 0; i < children.size(); i++) {
            String childname = children.get(i);
            zk.create(childname, childname.getBytes(), Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);

            Stat stat = new Stat();
            List<String> s = zk.getChildren(name, false, stat);

            Assert.assertEquals(stat.getCzxid(), stat.getMzxid());
            Assert.assertEquals(stat.getCzxid() + i + 1, stat.getPzxid());
            Assert.assertEquals(stat.getCtime(), stat.getMtime());
            Assert.assertEquals(i + 1, stat.getCversion());
            Assert.assertEquals(0, stat.getVersion());
            Assert.assertEquals(0, stat.getAversion());
            Assert.assertEquals(0, stat.getEphemeralOwner());
            Assert.assertEquals(name.length(), stat.getDataLength());
            Assert.assertEquals(i + 1, stat.getNumChildren());
            Assert.assertEquals(s.size(), stat.getNumChildren());
        }
        List<String> p = zk.getChildren(name, false, null);
        List<String> c_a = children_s;
        List<String> c_b = p;
        Collections.sort(c_a);
        Collections.sort(c_b);
        Assert.assertEquals(c_a.size(), 10);
        Assert.assertEquals(c_a, c_b);
    }
     */
  }
}