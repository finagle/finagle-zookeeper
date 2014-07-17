package com.twitter.finagle.exp.zookeeper.integration

import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.CreateMode
import com.twitter.finagle.exp.zookeeper.data.Ids
import com.twitter.finagle.exp.zookeeper.watcher.Watch
import com.twitter.util.Await
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class WatcherTest extends IntegrationConfig {
  test("Create, exists with watches , SetData") {
    newClient()
    connect()

    val res = for {
      _ <- client.get.create("/zookeeper/test", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
      exist <- client.get.exists("/zookeeper/test", true)
      setdata <- client.get.setData("/zookeeper/test", "CHANGE IS GOOD1".getBytes, -1)
    } yield (exist, setdata)

    val (exists, setData) = Await.result(res)
    Await.result(exists.watcher.get.event)

    exists.watcher.get.event onSuccess { rep =>
      assert(rep.typ === Watch.EventType.NODE_DATA_CHANGED)
      assert(rep.state === Watch.EventState.SYNC_CONNECTED)
      assert(rep.path === "/zookeeper/test")
    }

    disconnect()
    Await.ready(client.get.closeService())
  }

  test("Create, getData with watches , SetData") {
    newClient()
    connect()

    val res = for {
      _ <- client.get.create("/zookeeper/test", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
      getdata <- client.get.getData("/zookeeper/test", true)
      _ <- client.get.setData("/zookeeper/test", "CHANGE IS GOOD1".getBytes, -1)
    } yield getdata


    val ret = Await.result(res)
    Await.result(ret.watcher.get.event)
    ret.watcher.get.event onSuccess { rep =>
      assert(rep.typ === Watch.EventType.NODE_DATA_CHANGED)
      assert(rep.state === Watch.EventState.SYNC_CONNECTED)
      assert(rep.path === "/zookeeper/test")
    }

    disconnect()
    Await.ready(client.get.closeService())
  }

  test("Create, getChildren with watches , delete child") {
    newClient()
    connect()

    val res = for {
      _ <- client.get.create("/zookeeper/test", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
      _ <- client.get.create("/zookeeper/test/hello", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
      getchild <- client.get.getChildren("/zookeeper/test", true)
      _ <- client.get.delete("/zookeeper/test/hello", -1)
      _ <- client.get.delete("/zookeeper/test", -1)
    } yield getchild


    val ret = Await.result(res)
    Await.result(ret.watcher.get.event)
    ret.watcher.get.event onSuccess { rep =>
      assert(rep.typ === Watch.EventType.NODE_CHILDREN_CHANGED)
      assert(rep.state === Watch.EventState.SYNC_CONNECTED)
      assert(rep.path === "/zookeeper/test")
    }

    disconnect()
    Await.ready(client.get.closeService())
  }

  test("Create, getChildren2 with watches , delete child") {
    newClient()
    connect()

    val res = for {
      _ <- client.get.create("/zookeeper/test", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
      _ <- client.get.create("/zookeeper/test/hello", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
      getchild <- client.get.getChildren2("/zookeeper/test", true)
      _ <- client.get.delete("/zookeeper/test/hello", -1)
      _ <- client.get.delete("/zookeeper/test", -1)
    } yield getchild


    val ret = Await.result(res)
    Await.result(ret.watcher.get.event)
    ret.watcher.get.event onSuccess { rep =>
      assert(rep.typ === Watch.EventType.NODE_CHILDREN_CHANGED)
      assert(rep.state === Watch.EventState.SYNC_CONNECTED)
      assert(rep.path === "/zookeeper/test")
    }

    disconnect()
    Await.ready(client.get.closeService())
  }

  test("Create, getChildren(watcher on parent) exists(watcher on child) , delete child") {
    newClient()
    connect()

    val res = for {
      _ <- client.get.create("/zookeeper/test", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
      _ <- client.get.create("/zookeeper/test/hello", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
      _ <- client.get.create("/zookeeper/test/hella", "HELLO".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
      getchild <- client.get.getChildren("/zookeeper/test", true)
      exist <- client.get.exists("/zookeeper/test/hello", true)
      getdata <- client.get.getData("/zookeeper/test/hella", true)
      _ <- client.get.delete("/zookeeper/test/hello", -1)
      _ <- client.get.getChildren("/zookeeper/test", true)
      _ <- client.get.delete("/zookeeper/test/hella", -1)
      _ <- client.get.delete("/zookeeper/test", -1)
    } yield (getchild, exist)


    val (getChildrenRep, existsRep) = Await.result(res)

    Await.result(existsRep.watcher.get.event)
    existsRep.watcher.get.event onSuccess { rep =>
      assert(rep.typ === Watch.EventType.NODE_DELETED)
      assert(rep.state === Watch.EventState.SYNC_CONNECTED)
      assert(rep.path === "/zookeeper/test/hello")
    }

    Await.result(getChildrenRep.watcher.get.event)
    getChildrenRep.watcher.get.event onSuccess { rep =>
      assert(rep.typ === Watch.EventType.NODE_CHILDREN_CHANGED)
      assert(rep.state === Watch.EventState.SYNC_CONNECTED)
      assert(rep.path === "/zookeeper/test")
    }

    disconnect()
    Await.ready(client.get.closeService())
  }

  test("complete watcher test") {
    /*
    ZooKeeper zk = createClient(new CountdownWatcher(), hostPort);
        try {
            MyWatcher watchers[] = new MyWatcher[100];
            MyWatcher watchers2[] = new MyWatcher[watchers.length];
            for (int i = 0; i < watchers.length; i++) {
                watchers[i] = new MyWatcher();
                watchers2[i] = new MyWatcher();
                zk.create("/foo-" + i, ("foodata" + i).getBytes(),
                        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            Stat stat = new Stat();

            //
            // test get/exists with single set of watchers
            //   get all, then exists all
            //
            for (int i = 0; i < watchers.length; i++) {
                Assert.assertNotNull(zk.getData("/foo-" + i, watchers[i], stat));
            }
            for (int i = 0; i < watchers.length; i++) {
                Assert.assertNotNull(zk.exists("/foo-" + i, watchers[i]));
            }
            // trigger the watches
            for (int i = 0; i < watchers.length; i++) {
                zk.setData("/foo-" + i, ("foodata2-" + i).getBytes(), -1);
                zk.setData("/foo-" + i, ("foodata3-" + i).getBytes(), -1);
            }
            for (int i = 0; i < watchers.length; i++) {
                WatchedEvent event =
                    watchers[i].events.poll(10, TimeUnit.SECONDS);
                Assert.assertEquals("/foo-" + i, event.getPath());
                Assert.assertEquals(EventType.NodeDataChanged, event.getType());
                Assert.assertEquals(KeeperState.SyncConnected, event.getState());

                // small chance that an unexpected message was delivered
                //  after this check, but we would catch that next time
                //  we check events
                Assert.assertEquals(0, watchers[i].events.size());
            }

            //
            // test get/exists with single set of watchers
            //  get/exists together
            //
            for (int i = 0; i < watchers.length; i++) {
                Assert.assertNotNull(zk.getData("/foo-" + i, watchers[i], stat));
                Assert.assertNotNull(zk.exists("/foo-" + i, watchers[i]));
            }
            // trigger the watches
            for (int i = 0; i < watchers.length; i++) {
                zk.setData("/foo-" + i, ("foodata4-" + i).getBytes(), -1);
                zk.setData("/foo-" + i, ("foodata5-" + i).getBytes(), -1);
            }
            for (int i = 0; i < watchers.length; i++) {
                WatchedEvent event =
                    watchers[i].events.poll(10, TimeUnit.SECONDS);
                Assert.assertEquals("/foo-" + i, event.getPath());
                Assert.assertEquals(EventType.NodeDataChanged, event.getType());
                Assert.assertEquals(KeeperState.SyncConnected, event.getState());

                // small chance that an unexpected message was delivered
                //  after this check, but we would catch that next time
                //  we check events
                Assert.assertEquals(0, watchers[i].events.size());
            }

            //
            // test get/exists with two sets of watchers
            //
            for (int i = 0; i < watchers.length; i++) {
                Assert.assertNotNull(zk.getData("/foo-" + i, watchers[i], stat));
                Assert.assertNotNull(zk.exists("/foo-" + i, watchers2[i]));
            }
            // trigger the watches
            for (int i = 0; i < watchers.length; i++) {
                zk.setData("/foo-" + i, ("foodata6-" + i).getBytes(), -1);
                zk.setData("/foo-" + i, ("foodata7-" + i).getBytes(), -1);
            }
            for (int i = 0; i < watchers.length; i++) {
                WatchedEvent event =
                    watchers[i].events.poll(10, TimeUnit.SECONDS);
                Assert.assertEquals("/foo-" + i, event.getPath());
                Assert.assertEquals(EventType.NodeDataChanged, event.getType());
                Assert.assertEquals(KeeperState.SyncConnected, event.getState());

                // small chance that an unexpected message was delivered
                //  after this check, but we would catch that next time
                //  we check events
                Assert.assertEquals(0, watchers[i].events.size());

                // watchers2
                WatchedEvent event2 =
                    watchers2[i].events.poll(10, TimeUnit.SECONDS);
                Assert.assertEquals("/foo-" + i, event2.getPath());
                Assert.assertEquals(EventType.NodeDataChanged, event2.getType());
                Assert.assertEquals(KeeperState.SyncConnected, event2.getState());

                // small chance that an unexpected message was delivered
                //  after this check, but we would catch that next time
                //  we check events
                Assert.assertEquals(0, watchers2[i].events.size());
            }

        } finally {
            if (zk != null) {
                zk.close();
            }
        }
    }
     */
  }

  test("child watcher auto reset with chroot") {
    /*
    public void testChildWatcherAutoResetWithChroot() throws Exception {
        ZooKeeper zk1 = createClient();

        zk1.create("/ch1", null, Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);

        MyWatcher watcher = new MyWatcher();
        ZooKeeper zk2 = createClient(watcher, hostPort + "/ch1");
        zk2.getChildren("/", true );

        // this call shouldn't trigger any error or watch
        zk1.create("/youdontmatter1", null, Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        // this should trigger the watch
        zk1.create("/ch1/youshouldmatter1", null, Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        WatchedEvent e = watcher.events.poll(TIMEOUT, TimeUnit.MILLISECONDS);
        Assert.assertNotNull(e);
        Assert.assertEquals(EventType.NodeChildrenChanged, e.getType());
        Assert.assertEquals("/", e.getPath());

        MyWatcher childWatcher = new MyWatcher();
        zk2.getChildren("/", childWatcher);

        stopServer();
        watcher.waitForDisconnected(3000);
        startServer();
        watcher.waitForConnected(3000);

        // this should trigger the watch
        zk1.create("/ch1/youshouldmatter2", null, Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        e = childWatcher.events.poll(TIMEOUT, TimeUnit.MILLISECONDS);
        Assert.assertNotNull(e);
        Assert.assertEquals(EventType.NodeChildrenChanged, e.getType());
        Assert.assertEquals("/", e.getPath());
    }
     */
  }

  test("auto reset with chroot") {
    /*
    public void testDefaultWatcherAutoResetWithChroot() throws Exception {
        ZooKeeper zk1 = createClient();

        zk1.create("/ch1", null, Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);

        MyWatcher watcher = new MyWatcher();
        ZooKeeper zk2 = createClient(watcher, hostPort + "/ch1");
        zk2.getChildren("/", true );

        // this call shouldn't trigger any error or watch
        zk1.create("/youdontmatter1", null, Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        // this should trigger the watch
        zk1.create("/ch1/youshouldmatter1", null, Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        WatchedEvent e = watcher.events.poll(TIMEOUT, TimeUnit.MILLISECONDS);
        Assert.assertNotNull(e);
        Assert.assertEquals(EventType.NodeChildrenChanged, e.getType());
        Assert.assertEquals("/", e.getPath());

        zk2.getChildren("/", true );

        stopServer();
        watcher.waitForDisconnected(3000);
        startServer();
        watcher.waitForConnected(3000);

        // this should trigger the watch
        zk1.create("/ch1/youshouldmatter2", null, Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        e = watcher.events.poll(TIMEOUT, TimeUnit.MILLISECONDS);
        Assert.assertNotNull(e);
        Assert.assertEquals(EventType.NodeChildrenChanged, e.getType());
        Assert.assertEquals("/", e.getPath());
    }
     */
  }

  test("deep auto reset with chroot") {
    /*
  public void testDeepChildWatcherAutoResetWithChroot() throws Exception {
        ZooKeeper zk1 = createClient();

        zk1.create("/ch1", null, Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        zk1.create("/ch1/here", null, Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        zk1.create("/ch1/here/we", null, Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        zk1.create("/ch1/here/we/are", null, Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        MyWatcher watcher = new MyWatcher();
        ZooKeeper zk2 = createClient(watcher, hostPort + "/ch1/here/we");
        zk2.getChildren("/are", true );

        // this should trigger the watch
        zk1.create("/ch1/here/we/are/now", null, Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        WatchedEvent e = watcher.events.poll(TIMEOUT, TimeUnit.MILLISECONDS);
        Assert.assertNotNull(e);
        Assert.assertEquals(EventType.NodeChildrenChanged, e.getType());
        Assert.assertEquals("/are", e.getPath());

        MyWatcher childWatcher = new MyWatcher();
        zk2.getChildren("/are", childWatcher);

        stopServer();
        watcher.waitForDisconnected(3000);
        startServer();
        watcher.waitForConnected(3000);

        // this should trigger the watch
        zk1.create("/ch1/here/we/are/again", null, Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        e = childWatcher.events.poll(TIMEOUT, TimeUnit.MILLISECONDS);
        Assert.assertNotNull(e);
        Assert.assertEquals(EventType.NodeChildrenChanged, e.getType());
        Assert.assertEquals("/are", e.getPath());
    }
     */
  }
}
