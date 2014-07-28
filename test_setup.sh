#!/bin/bash

HERE=$(pwd)
BIN=$HERE/bin
DATADIR=$HERE/data
ZOOKEEPER=$BIN/zookeeper
ZOOKEEPER_VERSION=3.4.6

echo "Installing Zookeeper"
mkdir -p $BIN
mkdir -p $DATADIR
cd $BIN && \
curl -C - http://apache.osuosl.org/zookeeper/zookeeper-$ZOOKEEPER_VERSION/zookeeper-$ZOOKEEPER_VERSION.tar.gz | tar -zx
mv $BIN/zookeeper-$ZOOKEEPER_VERSION $ZOOKEEPER
chmod a+x $ZOOKEEPER/bin/zkServer.sh
cd $ZOOKEEPER/conf
    cat > zoo.cfg <<EOF
tickTime=2000
initLimit=10
syncLimit=5
dataDir=$DATADIR
clientPort=2181
EOF
echo "Finished installing Zookeeper"
echo "Starting ZooKeeper"
../bin/zkServer.sh start
