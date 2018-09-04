#!/bin/bash

sh install-py-deps.sh
sh install-spark.sh
export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)
source activate py36-twitter-mining
./spark-2.3.1-bin-hadoop2.7/bin/pyspark
