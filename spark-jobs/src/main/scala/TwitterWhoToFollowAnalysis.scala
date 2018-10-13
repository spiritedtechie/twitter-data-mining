import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._

object TwitterWhoToFollowAnalysis {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("TwitterWhoToFollowAnalysis")
    val streamingContext = new StreamingContext(sparkConf, Seconds(2))

    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9094",
      ConsumerConfig.GROUP_ID_CONFIG -> "who-to-follow-analysis-group",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (true: java.lang.Boolean),
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )
    val stream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](
        streamingContext,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Set("twitter-data"), kafkaParams)
      )

    val lines = stream.map(record => record.key)
    lines.print()

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}