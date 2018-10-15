# Description
An excuse to explore some interesting data technologies like Spark, Kafka, and some ML approaches


# Package and submit Spark job 
mvn clean package -P spark-submit
 
mvn exec:exec@submit-to-spark-local -pl spark-jobs -P spark-submit