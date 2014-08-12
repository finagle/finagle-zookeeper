#!/bin/sh

HERE=$(pwd)
S1ROOT=$HERE/s1
S2ROOT=$HERE/s2
S3ROOT=$HERE/s3
ZOOKEEPER_VERSION=$1

>&2 echo "Stopping server 1"
cd $S1ROOT/bin/zookeeper/bin/
./zkServer.sh stop
sleep 2
>&2 echo "Removing temp directories for server 1"
rm -rf $S1ROOT

>&2 echo "Stopping server 2"
cd $S2ROOT/bin/zookeeper/bin/
./zkServer.sh stop
sleep 2
>&2 echo "Removing temp directories for server 2"
rm -rf $S2ROOT

>&2 echo "Stopping server 3"
cd $S3ROOT/bin/zookeeper/bin/
./zkServer.sh stop
sleep 2
>&2 echo "Removing temp directories for server 3"
rm -rf $S3ROOT