#!/bin/sh

HERE=$(pwd)
S1ROOT=$HERE/s1
S2ROOT=$HERE/s2
S3ROOT=$HERE/s3
RELEASEDIR=$HERE/releases
ZOOKEEPER_VERSION=$1

echo "Installing Zookeeper"
>&2 echo "Installing Zookeeper v"$ZOOKEEPER_VERSION "(quorum mode)"

# Server 1
mkdir $S1ROOT
mkdir -p $S1ROOT/bin
mkdir -p $S1ROOT/data
echo 1 > $S1ROOT/data/myid

# Server 2
mkdir $S2ROOT
mkdir -p $S2ROOT/bin
mkdir -p $S2ROOT/data
echo 2 > $S2ROOT/data/myid

# Server 3
mkdir $S3ROOT
mkdir -p $S3ROOT/bin
mkdir -p $S3ROOT/data
echo 3 > $S3ROOT/data/myid

# Download and build zookeeper version
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

# Extract and build
if [ ! -d zookeeper-$ZOOKEEPER_VERSION ]; then
    tar -zxf release-$ZOOKEEPER_VERSION.tar.gz
    mv zookeeper-release-$ZOOKEEPER_VERSION zookeeper-$ZOOKEEPER_VERSION
    cd zookeeper-$ZOOKEEPER_VERSION/
    ant package
    cp build/zookeeper-$ZOOKEEPER_VERSION.jar zookeeper-$ZOOKEEPER_VERSION.jar
    cp -R build/lib/ lib
fi

# Configure S1
cd $RELEASEDIR/zookeeper-$ZOOKEEPER_VERSION/
cp -R ../zookeeper-$ZOOKEEPER_VERSION $S1ROOT/bin/zookeeper
chmod a+x $S1ROOT/bin/zookeeper/bin/zkServer.sh
cd $S1ROOT/bin/zookeeper/conf
cat > zoo.cfg <<EOF
tickTime=2000
initLimit=10
syncLimit=5
dataDir=$S1ROOT/data
clientPort=2181
server.1=127.0.0.1:2222:2223
server.2=127.0.0.1:3333:3334
server.3=127.0.0.1:4444:4445
EOF

# Configure S2
cd $RELEASEDIR/zookeeper-$ZOOKEEPER_VERSION/
cp -R ../zookeeper-$ZOOKEEPER_VERSION $S2ROOT/bin/zookeeper
chmod a+x $S2ROOT/bin/zookeeper/bin/zkServer.sh
cd $S2ROOT/bin/zookeeper/conf
cat > zoo.cfg <<EOF
tickTime=2000
initLimit=10
syncLimit=5
dataDir=$S2ROOT/data
clientPort=2182
server.1=127.0.0.1:2222:2223
server.2=127.0.0.1:3333:3334
server.3=127.0.0.1:4444:4445
EOF

# Configure S3
cd $RELEASEDIR/zookeeper-$ZOOKEEPER_VERSION/
cp -R ../zookeeper-$ZOOKEEPER_VERSION $S3ROOT/bin/zookeeper
chmod a+x $S3ROOT/bin/zookeeper/bin/zkServer.sh
cd $S3ROOT/bin/zookeeper/conf
cat > zoo.cfg <<EOF
tickTime=2000
initLimit=10
syncLimit=5
dataDir=$S3ROOT/data
clientPort=2183
server.1=127.0.0.1:2222:2223
server.2=127.0.0.1:3333:3334
server.3=127.0.0.1:4444:4445
EOF

>&2 echo "Finished installing v"$ZOOKEEPER_VERSION "(quorum mode)"
>&2 echo "Starting Zookeeper v"$ZOOKEEPER_VERSION "(quorum mode)"

>&2 echo "Starting server 1"
cd $S1ROOT/bin/zookeeper/bin/
./zkServer.sh start

>&2 echo "Starting server 2"
cd $S2ROOT/bin/zookeeper/bin/
./zkServer.sh start

>&2 echo "Starting server 3"
cd $S3ROOT/bin/zookeeper/bin/
./zkServer.sh start
sleep 15