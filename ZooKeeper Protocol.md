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
- `zxid`
- `err`

## Stat
- `czxid`
- `mzxid`
- `ctime`
- `mtime`
- `version`
- `cversion`
- `aversion`
- `ephemeralOwner`
- `dataLength`
- `numChildren`
- `pzxid`

## Watcher Event
- `typ`
- `state`
- `path`