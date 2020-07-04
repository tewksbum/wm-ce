package streamer

import java.nio.charset.StandardCharsets

import streamer.DatabaseConverter.saveRDDToDB
import streamer.DatabaseConverter.saveRawToDB
import streamer.OrderProcessor.processOrders
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.pubsub.{PubsubUtils, SparkGCPCredentials, SparkPubsubMessage}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object OrderStreamer {

  def createContext(projectID: String, windowLength: String, slidingInterval: String, checkpointDirectory: String)
    : StreamingContext = {

    // [START stream_setup]
    val sparkConf = new SparkConf().setAppName("NetsuiteOrderStreamer")
    val ssc = new StreamingContext(sparkConf, Seconds(slidingInterval.toInt))

    // // Set the checkpoint directory
    // val yarnTags = sparkConf.get("spark.yarn.tags")
    // val jobId = yarnTags.split(",").filter(_.startsWith("dataproc_job")).head
    // ssc.checkpoint(checkpointDirectory + '/' + jobId)
    
    // Create stream
    val pubsubStream: ReceiverInputDStream[SparkPubsubMessage] = PubsubUtils
      .createStream(
        ssc,
        projectID,
        None,
        "wm-order-intake-sparkles",  // Cloud Pub/Sub subscription name
        SparkGCPCredentials.builder.build(), 
        StorageLevel.MEMORY_AND_DISK_SER_2)
      
    // [END stream_setup]
    var messagesStream : DStream[String] = pubsubStream.map(message => new String(message.getData(), StandardCharsets.UTF_8))
    //process the stream
    processOrders(messagesStream,
      windowLength.toInt,
      slidingInterval.toInt,
      //decoupled handler that saves each separate result for processed to database
      saveRDDToDB(_, windowLength.toInt)
    )
    
	  ssc
  }

  def main(args: Array[String]): Unit = {
    // if (args.length != 5) {
    //   System.err.println(
    //     """
    //       | Usage: TrendingHashtags <projectID> <windowLength> <slidingInterval> <totalRunningTime>
    //       |
    //       |     <projectID>: ID of Google Cloud project
    //       |     <windowLength>: The duration of the window, in seconds
    //       |     <slidingInterval>: The interval at which the window calculation is performed, in seconds
    //       |     <totalRunningTime>: Total running time for the application, in minutes. If 0, runs indefinitely until termination.
    //       |     <checkpointDirectory>: Directory used to store RDD checkpoint data
    //       |
    //     """.stripMargin)
    //   System.exit(1)
    // }

    val windowLength = "10" // 10 second
    val projectID = "wemade-core"
    val slidingInterval = "10"
    val totalRunningTime = "0"
    var checkpointDirectory = "/tmp"

    // Create Spark context
    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => createContext(projectID, windowLength, slidingInterval, checkpointDirectory))

    // Start streaming until we receive an explicit termination
    ssc.start()

    if (totalRunningTime.toInt == 0) {
      ssc.awaitTermination()
    }
    else {
      ssc.awaitTerminationOrTimeout(1000 * 60 * totalRunningTime.toInt)
    }
  }

}
