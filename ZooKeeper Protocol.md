# ZooKeeper protocol documentation
## Connection
TODO
### Reconnection after connection lost
TODO
### Reconnection after session expired
TODO
## Request
TODO
## Response
TODO
## Watcher Event
TODO
# Glossary
TODO
## Connection
- `protocolVersion`
- `timeOut`
- `sessionId`
- `passwd`
- `canRO`

## ACL
- `perms`

### ID
- `scheme`
- `id`

## Reply Header
- `xid`
- `zxid` Every change to the ZooKeeper state receives a stamp in the form of a zxid (ZooKeeper Transaction Id). This exposes the total ordering of all changes to ZooKeeper. Each change will have a unique zxid and if zxid1 is smaller than zxid2 then zxid1 happened before zxid2.
- `err`

## Stat
- `czxid` The zxid of the change that caused this znode to be created.
- `mzxid` The zxid of the change that last modified this znode.
- `ctime` The time in milliseconds from epoch when this znode was created.
- `mtime` The time in milliseconds from epoch when this znode was last modified.
- `version` The number of changes to the data of this znode.
- `cversion` The number of changes to the children of this znode.
- `aversion` The number of changes to the ACL of this znode.
- `ephemeralOwner` The session id of the owner of this znode if the znode is an ephemeral node. If it is not an ephemeral node, it will be zero.
- `dataLength` The length of the data field of this znode.
- `numChildren` The number of children of this znode.
- `pzxid`

## Watcher Event
- `typ`
- `state`
- `path`

[Source](http://zookeeper.apache.org/doc/r3.2.1/zookeeperProgrammers.html)