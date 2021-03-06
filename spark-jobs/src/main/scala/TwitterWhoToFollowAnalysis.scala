import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.types.{StructType, _}


object TwitterWhoToFollowAnalysis {

  // Schema
  val tweetSchema = (new StructType)
    .add("id", StringType)
    .add("user", new StructType()
      .add("screen_name", StringType)
    )
    .add("retweeted_status", (new StructType)
      .add("id", StringType)
      .add("user", (new StructType)
        .add("screen_name", StringType)
        .add("following", BooleanType)
      )
      .add("retweet_count", IntegerType)
      .add("favorite_count", IntegerType)
    )

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


    // Convert JSON string to parsed JSON via Schema
    val tweetsDF: DataFrame = kafkaDF
      .selectExpr("cast (value as string) as tweetJson")
      .select(from_json($"tweetJson", tweetSchema).alias("tweet"))

    // Create a temp view for native SQL querying
    tweetsDF.createTempView("tweets")

    // Standard SQL approach, querying temp view
    val retweetsOfUnconnectedUsersDF = spark.sql("" +
      "SELECT tweet.id as tweet_id," +
      "       tweet.user as tweet_user, " +
      "       tweet.retweeted_status.id as retweeted_tweet_id, " +
      "       tweet.retweeted_status.user as retweeted_tweet_user, " +
      "       tweet.retweeted_status.retweet_count as retweeted_tweet_retweet_count, " +
      "       tweet.retweeted_status.favorite_count as retweeted_tweet_favorite_count " +
      "FROM tweets " +
      "WHERE tweet.retweeted_status IS NOT NULL " +
      "AND tweet.retweeted_status.user.following = false "
    ).distinct()

    retweetsOfUnconnectedUsersDF.createTempView("retweets_of_unconnected_users")

    val unconnectedUserRetweetCountDF = spark.sql("" +
      "SELECT retweeted_tweet_user, " +
      "       COUNT(*) as number_connections_who_retweeted, " +
      "       MAX(retweeted_tweet_retweet_count) as max_retweet, " +
      "       MAX(retweeted_tweet_favorite_count) as max_favourite " +
      "FROM retweets_of_unconnected_users " +
      "GROUP BY retweeted_tweet_user " +
      "ORDER BY max_retweet DESC"
    )

    val query: StreamingQuery = unconnectedUserRetweetCountDF
      .writeStream
      .outputMode(OutputMode.Complete())
      .format("console")
      .start()

    query.awaitTermination()
  }

}