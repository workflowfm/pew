#!/bin/bash

# Provide root kafka install directory as 1st parameter!
if [ ! -d "$1" ]; then
	echo "Provided Kafka directory doesn't exist."
	exit
fi

# Ensure the directories exists for zookeeper/kafka output files.
mkdir -p ./data 
mkdir -p ./data/log

# zookeeper.properties editted to use ./data dir.
$1/bin/zookeeper-server-start.sh ./config/zookeeper.properties &

sleep 10 # Give Zookeeper some time to boot up.

# server.properties editted to use ./data/log as log directory.
$1/bin/kafka-server-start.sh ./config/server.properties &
