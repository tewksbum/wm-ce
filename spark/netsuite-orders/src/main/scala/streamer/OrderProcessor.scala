package streamer

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.DefaultFormats

object OrderProcessor {

  implicit val formats = DefaultFormats

  // [START extract]
  private[streamer] def extractNetsuiteOrder(input: RDD[String]): RDD[NetsuiteOrder] = input.map( x => parse(x).extract[NetsuiteOrder])

  def processOrders(input: DStream[String],
                              windowLength: Int,
                              slidingInterval: Int,
                              handler: Array[NetsuiteOrder] => Unit): Unit = {
    val orders: DStream[NetsuiteOrder] = input
      .window(Seconds(windowLength), Seconds(slidingInterval)) //create a window
      .transform(extractNetsuiteOrder(_)) //apply transformation

    orders.foreachRDD(rdd => {
      val sqlContext = SqlContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._
      val df = rdd.toDF()
      df.printSchema()
      handler(rdd.collect()) //take top N hashtags and save to external source
    })
  }
}
