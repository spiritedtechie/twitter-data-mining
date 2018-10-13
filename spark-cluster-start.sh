#!/usr/bin/env bash
set -e

./spark/sbin/start-master.sh --webui-port 8080 --port 7077 --host localhost
./spark/sbin/start-slave.sh spark://localhost:7077
