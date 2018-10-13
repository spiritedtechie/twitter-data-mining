#!/usr/bin/env bash

# Download and install
SPARK_VERSION="spark-2.3.1-bin-hadoop2.7"

wget -nc http://apache.mirror.anlx.net/spark/spark-2.3.1/${SPARK_VERSION}.tgz

if [ ! -d "./${SPARK_VERSION}" ]
then
    tar -xvf ${SPARK_VERSION}.tgz
    ln -s ${SPARK_VERSION} spark
fi

