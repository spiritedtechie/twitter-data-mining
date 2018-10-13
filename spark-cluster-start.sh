#!/usr/bin/env bash
set -e

./spark/sbin/start-master.sh --webui-port 8080 --port 7077 --host `hostname`
./spark/sbin/start-slave.sh spark://`hostname`:7077
