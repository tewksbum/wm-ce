package streamer

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.DefaultFormats
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.SaveMode
import java.util.UUID.randomUUID
import java.text.SimpleDateFormat
import org.apache.spark.sql.functions._


object OrderProcessor {
  implicit val formats = DefaultFormats

  // [START extract]
  def extractNetsuiteOrder(input: RDD[String]): RDD[NetsuiteOrder] = {
    // input.collect().foreach(println)
    return input.map( 
      x => parse(x).extract[List[NetsuiteOrder]] // get list of orders
    ).flatMap(list => list) // now i should have RDD[NetsuiteOrder]
  }

  def processOrders(input: DStream[String], windowLength: Int, slidingInterval: Int, transformer: DataFrame => Unit): Unit = {
    val orders: DStream[NetsuiteOrder] = input
      .window(Seconds(windowLength), Seconds(slidingInterval)) //create a window
      .transform(extractNetsuiteOrder(_)) //apply transformation

    // orders.repartition(5) // this does not do anything

    orders.foreachRDD(rdd => {
      val sqlContext = SqlContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._      
      transformer(rdd.toDF)
    })

    if (OrderStreamer.runOnce) {
      input.context.stop()
    }
  }
}
