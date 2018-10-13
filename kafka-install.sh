#!/usr/bin/env bash

# Download and install kafka
KAFKA_VERSION="kafka_2.11-2.0.0"

wget -nc http://apache.mirror.anlx.net/kafka/2.0.0/${KAFKA_VERSION}.tgz

if [ ! -d "./${KAFKA_VERSION}" ]
then
    tar -xvf ${KAFKA_VERSION}.tgz
    ln -s ${KAFKA_VERSION} kafka
fi

# Make data directories
sudo mkdir -p /var/lib/zookeeper
sudo mkdir -p /var/lib/kafka
sudo chown $USER /var/lib/zookeeper
sudo chown $USER /var/lib/kafka