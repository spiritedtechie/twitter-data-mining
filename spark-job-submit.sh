#!/usr/bin/env bash
set -e

./spark/bin/spark-submit \
    --deploy-mode cluster \
    --supervise \
    --master spark://localhost:7077 \
    spark-jobs/target/twitter-data-mining-spark-jobs-1.0-SNAPSHOT.jar