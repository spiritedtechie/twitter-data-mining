#!/usr/bin/env bash

# Start kafka cluster and zookeeper node
./kafka/bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
./kafka/bin/kafka-server-start.sh -daemon config/server-1.properties
./kafka/bin/kafka-server-start.sh -daemon config/server-2.properties 

sleep 5

# Create topics - 2 partitions and 1 replica for the simple setup of two local nodes
./kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic twitter-data

# Describe topic setup
./kafka/bin/kafka-topics.sh --list --zookeeper localhost:2181
./kafka/bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic twitter-data

