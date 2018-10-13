#!/usr/bin/env bash
./kafka/bin/kafka-console-consumer.sh --topic twitter-data --bootstrap-server  localhost:9093 --from-beginning