package streamer

import java.sql.{Connection => DbConnection}
import java.nio.charset.StandardCharsets

import streamer.OrderProcessor.processOrders
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.pubsub.{PubsubUtils, SparkGCPCredentials, SparkPubsubMessage}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql._
import DataTransformer.transformOrders;

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
  // var jdbcMariadbUrl: String = "jdbc:mariadb://10.128.0.32:3306/segment"
  var jdbcMysqlUrl: String = "jdbc:mysql://10.45.160.5:3306/segment"
  var jdbcMssqlUrl: String = "jdbc:jtds:sqlserver://10.128.0.32:1433/segment";

  val jdbcUserName: String = "spark"
  val jdbcPassword: String = "sMehnXVuJ0LKQcndEtvv"

  val jdbcWriteProperties = new java.util.Properties()
  jdbcWriteProperties.setProperty("driver", "org.mariadb.jdbc.Driver")

  val jdbcReadProperties = new java.util.Properties()
  jdbcReadProperties.setProperty("driver", "com.mysql.cj.jdbc.Driver")

  // scd cached
  var dimDestTypes: DataFrame = _
  var dimDates: DataFrame = _
  var dimProducts: DataFrame = _
  var dimLobs: DataFrame = _
  var dimSchools: DataFrame = _
  var dimSources: DataFrame = _
  var dimChannels: DataFrame = _
  var dimSchedules: DataFrame = _

  var runOnce: Boolean = false

  def createContext(
      projectID: String,
      windowLength: Int,
      slidingInterval: Int
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
    var messagesStream: DStream[String] =
      pubsubStream.map(message => new String(message.getData(), StandardCharsets.UTF_8))
    //process the stream
    processOrders(messagesStream, windowLength, slidingInterval, transformOrders(_, ssc))

    ssc
  }

  def main(args: Array[String]): Unit = {
    for(arg<-args) 
    { 
        if (arg == "runonce") {
          runOnce = true
        }
    } 

    val config = ConfigFactory.load();
    projectID = config.getString("project.id")
    projectID = config.getString("project.id")
    subscription = config.getString("pubsub.subscription")
    windowLength = config.getInt("pubsub.windowlength")
    slidingInterval = config.getInt("pubsub.slidinginterval")
    totalRunningTime = config.getInt("pubsub.runningtime")
    checkpointDirectory = config.getString("spark.checkpoint.dir")
    secretVersion = config.getString("config.secretversion ")
    // jdbcMariadbUrl = config.getString("config.mariadburl")
    jdbcMysqlUrl = config.getString("config.mysqlurl")

    // // read the secret
    // val smServiceSettings = SecretManagerServiceSettings.newBuilder().build()
    // val smClient = SecretManagerServiceClient.create(smServiceSettings)

    // val secretResponse = smClient.accessSecretVersion(secretVersion)
    // val jdbcurl = secretResponse.getPayload().getData().toStringUtf8()
    // println(jdbcurl)

    // Create Spark context
    val ssc = StreamingContext.getOrCreate(
      checkpointDirectory,
      () => createContext(projectID, windowLength, slidingInterval)
    )

    // load some dataframes
    val sqlContext = new org.apache.spark.sql.SQLContext(ssc.sparkContext)
    import sqlContext.implicits._
    println("preloading dim_dates")
    dimDates = sqlContext.read.jdbc(
      jdbcMysqlUrl,
      "(select date_key, str_to_date(date, '%Y-%m-%d') as date_string from dim_dates where date < current_date + interval 7 day) dates",
      jdbcReadProperties
    )
    dimDates.cache().count() // force it to load

    println("preloading dim_products")
    dimProducts = sqlContext.read.jdbc(
      jdbcMysqlUrl,
      "(select product_key, sku, lob_key, netsuite_id from dim_products) products",
      jdbcReadProperties
    )
    dimProducts.cache().count() // force it to load

    println("preloading dim_lobs")
    dimLobs = sqlContext.read.jdbc(jdbcMysqlUrl, "dim_lobs", jdbcReadProperties)
    dimLobs.cache().count() // force it to load

    println("preloading dim_schools")
    dimSchools = sqlContext.read.jdbc(
      jdbcMysqlUrl,
      "(select school_key, school_code, school_name, netsuite_id from dim_schools) schools",
      jdbcReadProperties
    )
    dimSchools.cache().count() // force it to load

    println("preloading dim_sources")
    dimSources = sqlContext.read.jdbc(jdbcMysqlUrl, "dim_sources", jdbcReadProperties)
    dimSources.cache().count() // force it to load

    println("preloading dim_channels")
    dimChannels = sqlContext.read.jdbc(jdbcMysqlUrl, "dim_channels", jdbcReadProperties)
    dimChannels.cache().count() // force it to load

    println("preloading dim_schedules")
    dimSchedules = sqlContext.read.jdbc(jdbcMysqlUrl, "dim_schedules", jdbcReadProperties)
    dimSchedules.cache().count() // force it to load

    println("preloading dim_desttypes")
    dimDestTypes = sqlContext.read.jdbc(jdbcMysqlUrl, "dim_desttypes", jdbcReadProperties)
    dimDestTypes.cache().count() // force it to load

    // Start streaming until we receive an explicit termination
    ssc.start()

    if (totalRunningTime.toInt == 0) {
      ssc.awaitTermination()
    } else {
      ssc.awaitTerminationOrTimeout(1000 * 60 * totalRunningTime.toInt)
    }
  }

}
