#!/bin/sh

HERE=$(pwd)
BIN=$HERE/bin
DATADIR=$HERE/data
ZOOKEEPER_VERSION=$1

>&2 echo "Stopping server v"$ZOOKEEPER_VERSION "(standalone mode)"
cd $BIN/zookeeper/bin/
./zkServer.sh stop
sleep 2
>&2 echo "Removing temp directories"
rm -rf $BIN
rm -rf $DATADIR