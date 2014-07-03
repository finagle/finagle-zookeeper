# THE DOCUMENTATION IS OUTDATED, see integration test for examples

# finagle-zookeeper

finagle-zookeeper provides basic tools to communicate with a Zookeeper server asynchronously.

## Architecture
### Client
- `Client` is based on Finagle 6 client model.


### Common
- `Data` represents ACL, ID structures with associated serialization/deserialization definitions
- `Request` contains every request with serialization definitions
- `Response` contains every response with deserialization definitions
- `ZooKeeper` contains DefaultClient definition (Bridge, dispatcher)
- `ZookeeperDefinitions` contains zookeeper code definitions

## Commands

Every request returns a *twitter.util.Future* (see [Effective Scala](http://twitter.github.io/effectivescala/#Concurrency-Futures),
[Finagle documentation](https://twitter.github.io/scala_school/finagle.html#Future) and [Scaladoc](http://twitter.github.io/util/util-core/target/doc/main/api/com/twitter/util/Future.html))

Here is the list of commands supported by version 0.1 :

## Test
See src/test/scala

### Client creation
```
  val client = ZooKeeper.newRichClient("127.0.0.1:2181")
```
- `127.0.0.1:2181` is a String representing the IP address of the Zookeeper server with the port separated with colon

### Connection
```
val connect = client.connect
    connect onSuccess {
      a =>
        logger.info("Connected to zookeeper server: " + client.adress)
    } onFailure {
      e =>
        logger.severe("Connect Error")
    }
```
logger is a java.util.logging.Logger

### Disconnect
```
client.disconnect
```

### First request
For an unknown reason please use a for comprehension when sending multiple request at the same time, otherwise your requests won't be sequentialize :

```
val res = for {
      acl <- client.getACL("/zookeeper")
      _ <- client.create("/zookeeper/test", "HELLO".getBytes, ACL.defaultACL, createMode.EPHEMERAL)
      _ <- client.exists("/zookeeper/test", true)
      _ <- client.setData("/zookeeper/test", "CHANGE".getBytes, -1)
    } yield (acl)
```

### About response
As there could be wrong request, the response is an Option[ResponseBody], so when onSuccess is triggered, you can get Some(ResponseBody) or None

### Create
```
val create = client.create("/zookeeper/testnode", "HELLO".getBytes, ACL.defaultACL, createMode.EPHEMERAL)
```
- `/zookeeper/testnode` : String the node that you want to create
- `"HELLO".getBytes` : Array[Byte] the data associated to this node
- `ACL.defaultACL` : Array[ACL] the ACL list
- `createMode.EPHEMERAL` : Int the creation mode

Return value `String` representing the path you have just created

- `createMode.PERSISTENT` persistent mode
- `createMode.EPHEMERAL` ephemeral mode
- `createMode.PERSISTENT_SEQUENTIAL` persistent and sequential mode
- `createMode.EPHEMERAL_SEQUENTIAL` ephemeral and sequential mode


### Delete
```
_ <- client.delete("/zookeeper/test", -1)
```
- `/zookeeper/test` : String the node that you want to delete
- `-1` : Int corresponding version of your data (-1 if you don't care)

Return value `Option[ReplyHeader]` `ReplyHeader(xid: Int, zxid: Long,err: Int)`

### Exists
```
_ <- client.exists("/zookeeper/test", false)
```
- `/zookeeper/test` : String the node that you want to test
- `false` : Boolean if you want to set a watch on this node

Return value `Option[ExistsResponseBody]` `ExistsResponseBody(stat: Stat)`

### Get ACL
```
client.getACL("/zookeeper")
```
- `/zookeeper` : String the node from which you want to retrieve ACL

Return value `Option[GetACLResponseBody]` `GetACLResponseBody(acl: Array[ACL], stat: Stat)`

### Set ACL
```
client.setACL("/zookeeper/test", ACL.defaultACL, -1)
```
- `/zookeeper/test` : String the node that you want to set
- `ACL.defaultACL` : Array[ACL] the ACL list
- `-1` : Int corresponding version of your data (-1 if you don't care)

Return value `Option[SetACLResponseBody]` `SetACLResponseBody(stat: Stat)`

### Get children
```
client.getChildren("/zookeeper", false)
```
- `/zookeeper` : String the node that you want to get
- `false` : Boolean if you want to set a watch on this node

Return value `Option[GetChildrenResponseBody]` `GetChildrenResponseBody(children: Array[String])`

### Get children2
```
client.getChildren2("/zookeeper", false)
```
- `/zookeeper` : String the node that you want to get
- `false` : Boolean if you want to set a watch on this node

Return value `Option[GetChildren2ResponseBody]` `GetChildren2ResponseBody(children: Array[String], stat:Stat)`

### Get Data
```
client.getData("/zookeeper/test", false)
```
- `/zookeeper/test` : String the node that you want to get
- `false` : Boolean if you want to set a watch on this node

Return value `Option[GetDataResponseBody]` `GetDataResponseBody(data: Array[Byte], stat: Stat)`

### Set Data
```
client.setData("/zookeeper/test", "CHANGE".getBytes, -1)
```
- `/zookeeper/test` : String the node that you want to set
- `"CHANGE".getBytes` : Array[Byte] data that you want to set on this node
- `-1` : Int corresponding version of your data (-1 if you don't care)

Return value `Option[SetDataResponseBody]` `SetDataResponseBody(stat: Stat)`

### Sync
```
client.sync("/zookeeper")
```
- `/zookeeper` : String the node that you want to sync

Return value `Option[SyncResponseBody]` `SyncResponseBody(path: String)`