import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._

object TwitterWhoToFollowAnalysis {

  def main(args: Array[String]) {

    val spark: SparkSession = SparkSession.builder
      .appName("TwitterWhoToFollowAnalysis")
      .getOrCreate()

    val streamingContext = new StreamingContext(spark.sparkContext, Seconds(2))

    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9094",
      ConsumerConfig.GROUP_ID_CONFIG -> "who-to-follow-analysis-group",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (true: java.lang.Boolean),
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    // Create stream (of continuous seq of RDDs)
    val stream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](
        streamingContext,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Set("twitter-data"), kafkaParams)
      )

    // Process RDDs in the stream
    stream.foreachRDD((rdd: RDD[ConsumerRecord[String, String]]) => {
      val tweetJsonRdd: RDD[String] = rdd.map(_.value())
      val tweetJsonDataset: Dataset[String] = spark.createDataset(tweetJsonRdd)(Encoders.STRING)
      val tweetDF: DataFrame = spark.read.json(tweetJsonDataset)
      tweetDF.createOrReplaceTempView("tweets")

      if (hasRetweets(tweetDF)) {
        val usersToFollowDF: DataFrame = spark.sql("" +
          "SELECT retweeted_status.id as retweeted_tweet_id, " +
          "       retweeted_status.user.screen_name as retweeted_user " +
          "FROM tweets " +
          "WHERE retweeted_status IS NOT NULL " +
          "AND retweeted_status.user.following = false"
        )

        usersToFollowDF.foreach(user => println("**** User to start following: " + user))
      }
    })

    // start context
    streamingContext.start()
    streamingContext.awaitTermination()
  }


  /** Since some tweets are not retweets, there will be datasets where there are no tweets that have no retweeted_status field.
    * We need to ignore datasets that meet that criteria and there is nothing useful in them
    * Also spark blows up if you try to query a dataset for a column that is not in the derived schema
    * */
  val hasRetweets: DataFrame => Boolean =
    (dataFrame: DataFrame) => dataFrame.schema.fields.exists(structField => "retweeted_status".equals(structField.name))
}