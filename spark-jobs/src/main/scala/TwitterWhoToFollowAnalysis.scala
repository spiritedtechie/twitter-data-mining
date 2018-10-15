import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.StructType


object TwitterWhoToFollowAnalysis {

  def main(args: Array[String]) {

    val spark: SparkSession = SparkSession.builder
      .appName("TwitterWhoToFollowAnalysis")
      .getOrCreate()

    import spark.implicits._

    val kafkaDF: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9094")
      .option("subscribe", "twitter-data")
      .option("startingOffsets", "earliest")
      .load()

    // Schema
    val tweetSchema = new StructType()
      .add("id", "string")
      .add("retweeted_status", new StructType()
        .add("id", "string")
        .add("user", new StructType()
          .add("screen_name", "string")
          .add("following", "boolean")
        )
      )

    // Convert JSON string to parsed JSON via Schema
    val tweetDF: DataFrame = kafkaDF
      .selectExpr("cast (value as string) as tweetJson")
      .select(from_json($"tweetJson", tweetSchema).alias("tweet"))

    // Create a temp view for native SQL querying
    tweetDF.createTempView("tweets")

    // Spark SQL approach
    val retweetsOfPeopleIDoNotFollowDFNotUsed = tweetDF
      .select("tweet.retweeted_status.id",
        "tweet.retweeted_status.user.screen_name")
      .where("tweet.retweeted_status is not null")
      .where("tweet.retweeted_status.user.following = false")

    // Standard SQL approach, querying temp view
    val retweetsOfPeopleIDoNotFollowDF = spark.sql("" +
      "SELECT tweet.retweeted_status.id as retweeted_tweet_id, " +
      "       tweet.retweeted_status.user.screen_name as retweeted_user " +
      "FROM tweets " +
      "WHERE tweet.retweeted_status IS NOT NULL " +
      "AND tweet.retweeted_status.user.following = false"
    )

    val query: StreamingQuery = retweetsOfPeopleIDoNotFollowDF.writeStream
      .format("console")
      .start()

    query.awaitTermination()
  }

}