#!/bin/bash

# check arguments
if [ $# != 0 ]; then
    echo "usage: $(basename $0)"
    exit
fi

# compute project directory and hostfile locations
projectdir="$(pwd)/$(dirname $0)/.."
hostfile="$projectdir/etc/hosts.txt"

# initialize instance variables
if [ -f "$projectdir/impl/stitchd/main.py" ]; then
    application="$projectdir/impl/stitchd/main.py"
fi

if [ -z "$application" ]; then
    echo "'stitchd' binary not found."
    exit
fi

# iterate over hosts
nodeid=0
while read line; do
    # parse host, port, and options
    host=$(echo $line | awk '{print $1}')
    rpcport=$(echo $line | awk '{print $2}')
    options=$(echo $line | cut -d' ' -f3-)

    echo "starting node $nodeid"
    if [ $host == "127.0.0.1" ]; then
        # start application locally
        RUST_LOG=debug $application -i $host -p $rpcport $options \
            > $projectdir/log/node-$nodeid.log 2>&1 &

        echo $! > $projectdir/log/node-$nodeid.pid
    else
        # start application on remote host
        ssh rammerd@$host -n "RUST_LOG=debug \
            $application -i $host -p $rpcport $options \
                > $projectdir/log/node-$nodeid.log 2>&1 & \
            echo \$! > $projectdir/log/node-$nodeid.pid"
    fi

    # increment node id
    (( nodeid += 1 ))
done <$hostfile
