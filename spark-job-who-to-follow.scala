import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._

object TwitterWhoToFollowAnalysis {

    def main(args: Array[String]) {

        val sparkConf = new SparkConf().setAppName("TwitterWhoToFollowAnalysis")
        val ssc = new StreamingContext(sparkConf, Seconds(2))

        val kafkaParams = Map[String, Object](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9091",
            ConsumerConfig.GROUP_ID_CONFIG -> "my-group",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
        )
        val messages = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Set("twitter-data"), kafkaParams)
        )


        ssc.start()
        ssc.awaitTermination()
    }

}