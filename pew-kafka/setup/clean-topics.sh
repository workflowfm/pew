#!/bin/bash

## DELETE ##

$1/bin/kafka-topics.sh --zookeeper localhost:2181 --delete \
            --topic Assignment

$1/bin/kafka-topics.sh --zookeeper localhost:2181 --delete \
            --topic Result

$1/bin/kafka-topics.sh --zookeeper localhost:2181 --delete \
            --topic PiiHistory

$1/bin/kafka-topics.sh --zookeeper localhost:2181 --delete \
            --topic ReduceRequest

## RESTORE ##

$1/bin/kafka-topics.sh --create --zookeeper localhost:2181 \
            --replication-factor 1 --partitions 12 \
            --topic Assignment

$1/bin/kafka-topics.sh --create --zookeeper localhost:2181 \
            --replication-factor 1 --partitions 4 \
            --topic Result

$1/bin/kafka-topics.sh --create --zookeeper localhost:2181 \
            --replication-factor 1 --partitions 4 \
            --topic PiiHistory

$1/bin/kafka-topics.sh --create --zookeeper localhost:2181 \
            --replication-factor 1 --partitions 4 \
            --topic ReduceRequest
