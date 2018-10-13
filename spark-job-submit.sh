#!/usr/bin/env bash
set -e

./spark/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.3.1 $1