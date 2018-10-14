import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._

object TwitterWhoToFollowAnalysis {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("TwitterWhoToFollowAnalysis")
    val sparkSession: SparkSession = SparkSession.builder.config(sparkConf).getOrCreate()
    val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(2))

    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9094",
      ConsumerConfig.GROUP_ID_CONFIG -> "who-to-follow-analysis-group",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (true: java.lang.Boolean),
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    // Create stream
    val stream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](
        streamingContext,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Set("twitter-data"), kafkaParams)
      )

    // Process stream
    stream.foreachRDD((rdd: RDD[ConsumerRecord[String, String]]) => {

      val rdd1: RDD[String] = rdd.map(cr => cr.value())

      val ds: Dataset[String] = sparkSession.createDataset(rdd1)(Encoders.STRING)

      val df: DataFrame = sparkSession.read.json(ds)


      if (df.count() >= 1) {

        val dfHasRetweets = !df.schema.fields.filter(sf => "retweeted_status".equals(sf.name)).isEmpty

        if (dfHasRetweets) {
          val rowsThatAreRetweets = df.select("id", "retweeted_status")
            .where("retweeted_status is not null")

          rowsThatAreRetweets.head(5).foreach(r => println("**** row: " + r))

        }

      }

    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}