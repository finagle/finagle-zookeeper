#!/bin/sh

HERE=$(pwd)
BIN=$HERE/bin
DATADIR=$HERE/data
RELEASEDIR=$HERE/releases
ZOOKEEPER_VERSION=$1

>&2 echo "Installing Zookeeper v"$ZOOKEEPER_VERSION "(standalone mode)"
mkdir -p $BIN
mkdir -p $DATADIR
if [ ! -d $RELEASEDIR/ ]; then
    mkdir -p $RELEASEDIR
fi

cd $RELEASEDIR

if [ ! -f $RELEASEDIR/release-$ZOOKEEPER_VERSION.tar.gz ]; then
    wget $2
    if [ -d zookeeper-$ZOOKEEPER_VERSION ]; then
        rm -rf zookeeper-$ZOOKEEPER_VERSION
    fi
fi

if [ ! -d zookeeper-$ZOOKEEPER_VERSION ]; then
    tar -zxf release-$ZOOKEEPER_VERSION.tar.gz
    mv zookeeper-release-$ZOOKEEPER_VERSION zookeeper-$ZOOKEEPER_VERSION
    cd zookeeper-$ZOOKEEPER_VERSION/
    ant package
    cp build/zookeeper-$ZOOKEEPER_VERSION.jar zookeeper-$ZOOKEEPER_VERSION.jar
    cp -R build/lib/ lib
fi

cd $RELEASEDIR/zookeeper-$ZOOKEEPER_VERSION/
cp -R ../zookeeper-$ZOOKEEPER_VERSION $BIN/zookeeper
chmod a+x $BIN/zookeeper/bin/zkServer.sh
cd $BIN/zookeeper/conf
cat > zoo.cfg <<EOF
tickTime=2000
initLimit=10
syncLimit=5
dataDir=$DATADIR
clientPort=2181
EOF
>&2 echo "Finished installing v"$ZOOKEEPER_VERSION "(standalone mode)"
>&2 echo "Starting Zookeeper v"$ZOOKEEPER_VERSION "(standalone mode)"
cd $BIN/zookeeper/bin/
./zkServer.sh start
sleep 5