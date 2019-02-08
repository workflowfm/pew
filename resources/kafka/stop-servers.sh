#!/bin/bash

# Jev, merely closing the ports doesn't work
# fuser 9092/tcp -k
# fuser 2181/tcp -k

# Provide root kafka install directory as 1st parameter!
if [ ! -d "$1" ]; then
	echo "Provided Kafka directory doesn't exist."
	exit
fi

# Jev, no reason to run in child processes, these run synchronously.
$1/bin/kafka-server-stop.sh
sleep 20 # Kafka doesn't seem to shutdown immediately,
$1/bin/zookeeper-server-stop.sh
