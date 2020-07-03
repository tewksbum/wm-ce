/*
 Copyright Google Inc. 2018
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 http://www.apache.org/licenses/LICENSE-2.0
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package streamer

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.DefaultFormats
import streamer.models.NetsuiteOrder

object OrderProcessor {

  implicit val formats = DefaultFormats

  // [START extract]
  private[streamer] def extractNetsuiteOrder(input: RDD[String]): RDD[NetsuiteOrder] = input.map( x => parse(x).extract[NetsuiteOrder])

  def processOrders(input: DStream[String],
                              windowLength: Int,
                              slidingInterval: Int,
                              //handler: Array[NetsuiteOrder] => Unit): Unit = {
                              handler: Array[NetsuiteOrder] => Unit): Unit = {
    val orders: DStream[NetsuiteOrder] = input
      .window(Seconds(windowLength), Seconds(slidingInterval)) //create a window
      .transform(extractNetsuiteOrder(_)) //apply transformation

    orders.foreachRDD(rdd => {
      handler(rdd.collect()) //take top N hashtags and save to external source
    })
  }
}
