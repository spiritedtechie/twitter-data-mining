# Description
An excuse to explore some interesting data technologies like Spark, Kafka, and some ML approaches eventually.


# Package Spark job
mvn clean package -P spark-submit


# Submit Spark job to a local (in-process) Spark instance
mvn exec:exec@submit-to-spark-local -pl spark-jobs -P spark-submit


# Development workflow

A useful one liner to package and submit:

    mvn clean package exec:exec@submit-to-spark-local -pl spark-jobs -P spark-submit >> spark-submit.log

And then to filter custom log entries:

    tail -F spark-submit.log | grep "\*\*\*"

Then open the browser at:

    http://localhost:4040