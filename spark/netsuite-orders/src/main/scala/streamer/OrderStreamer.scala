package streamer

import java.sql.{Connection => DbConnection}
import java.nio.charset.StandardCharsets

import streamer.OrderProcessor.processOrders
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.pubsub.{
  PubsubUtils,
  SparkGCPCredentials,
  SparkPubsubMessage
}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql._

import com.typesafe.config.ConfigFactory

// import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
// import com.google.cloud.secretmanager.v1.{SecretManagerServiceClient, SecretManagerServiceSettings}

object OrderStreamer {

  var projectID: String = "wemade-core"
  var subscription: String = "wm-order-intake-sparkles"
  var windowLength: Int = 30 // 10 second
  var slidingInterval: Int = 30
  var totalRunningTime: Int = 0
  var checkpointDirectory: String = "/tmp"
  var secretVersion: String = "projects/180297787522/secrets/mariadb/versions/1"
  var jdbcUrl: String =
    "jdbc:mariadb://10.128.0.32:3306/segment?user=spark&password=sMehnXVuJ0LKQcndEtvv"

  val jdbcProperties = new java.util.Properties()
  jdbcProperties.setProperty("driver", "org.mariadb.jdbc.Driver")

  // scd cached
  var dimDestTypes: DataFrame = _
  var dimDates: DataFrame = _
  var dimProducts: DataFrame = _
  var dimLobs: DataFrame = _
  var dimSchools: DataFrame = _
  var dimSources: DataFrame = _
  var dimChannels: DataFrame = _
  var dimSchedules: DataFrame = _

  def createContext(
      projectID: String,
      windowLength: Int,
      slidingInterval: Int,
      jdbcUrl: String
  ): StreamingContext = {

    // [START stream_setup]
    val sparkConf = new SparkConf().setAppName("NetsuiteOrderStreamer")
    val ssc = new StreamingContext(sparkConf, Seconds(slidingInterval))

    var gcpCred = SparkGCPCredentials.builder.build()

    // Create stream
    val pubsubStream: ReceiverInputDStream[SparkPubsubMessage] = PubsubUtils
      .createStream(
        ssc,
        projectID,
        None,
        subscription, // Cloud Pub/Sub subscription name
        gcpCred,
        StorageLevel.MEMORY_AND_DISK_SER_2
      )

    // [END stream_setup]
    var messagesStream: DStream[String] = pubsubStream.map(message =>
      new String(message.getData(), StandardCharsets.UTF_8)
    )
    //process the stream
    processOrders(messagesStream, windowLength, slidingInterval)

    ssc
  }

  def main(args: Array[String]): Unit = {

    val config = ConfigFactory.load();
    projectID = config.getString("project.id")
    projectID = config.getString("project.id")
    subscription = config.getString("pubsub.subscription")
    windowLength = config.getInt("pubsub.windowlength")
    slidingInterval = config.getInt("pubsub.slidinginterval")
    totalRunningTime = config.getInt("pubsub.runningtime")
    checkpointDirectory = config.getString("spark.checkpoint.dir")
    secretVersion = config.getString("config.secretversion ")
    jdbcUrl = config.getString("config.jdbcurl")

    // // read the secret
    // val smServiceSettings = SecretManagerServiceSettings.newBuilder().build()
    // val smClient = SecretManagerServiceClient.create(smServiceSettings)

    // val secretResponse = smClient.accessSecretVersion(secretVersion)
    // val jdbcurl = secretResponse.getPayload().getData().toStringUtf8()
    // println(jdbcurl)

    // Create Spark context
    val ssc = StreamingContext.getOrCreate(
      checkpointDirectory,
      () => createContext(projectID, windowLength, slidingInterval, jdbcUrl)
    )

    // load some dataframes
    val sqlContext = new org.apache.spark.sql.SQLContext(ssc.sparkContext)
    import sqlContext.implicits._

    dimDestTypes =
      sqlContext.read.jdbc(jdbcUrl, "dim_desttypes", jdbcProperties)
    dimDestTypes.cache().count() // force it to load
    dimDates = sqlContext.read.jdbc(
      jdbcUrl,
      "(select date_key, cast(date as varchar(10)) as date_string from dim_dates) dates",
      jdbcProperties
    )
    dimDates.cache().count() // force it to load
    dimProducts = sqlContext.read.jdbc(
      jdbcUrl,
      "(select product_key, sku, lob_key, netsuite_id from dim_products) products",
      jdbcProperties
    )
    dimProducts.cache().count() // force it to load
    dimLobs = sqlContext.read.jdbc(jdbcUrl, "dim_lobs", jdbcProperties)
    dimLobs.cache().count() // force it to load
    dimSchools = sqlContext.read.jdbc(
      jdbcUrl,
      "(select school_key, school_code, school_name, netsuite_id from dim_schools) schools",
      jdbcProperties
    )
    dimSchools.cache().count() // force it to load
    dimSources = sqlContext.read.jdbc(jdbcUrl, "dim_sources", jdbcProperties)
    dimSources.cache().count() // force it to load
    dimChannels = sqlContext.read.jdbc(jdbcUrl, "dim_channels", jdbcProperties)
    dimChannels.cache().count() // force it to load
    dimSchedules =
      sqlContext.read.jdbc(jdbcUrl, "dim_schedules", jdbcProperties)
    dimSchedules.cache().count() // force it to load

    // Start streaming until we receive an explicit termination
    ssc.start()

    if (totalRunningTime.toInt == 0) {
      ssc.awaitTermination()
    } else {
      ssc.awaitTerminationOrTimeout(1000 * 60 * totalRunningTime.toInt)
    }
  }

}
