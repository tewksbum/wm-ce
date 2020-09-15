package streamer

import com.google.api.core.ApiFuture
import com.google.api.core.ApiFutureCallback
import com.google.api.core.ApiFutures
import com.google.cloud.pubsub.v1.Publisher
import com.google.pubsub.v1.ProjectTopicName
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.DefaultFormats
import org.json4s.native.Serialization.{read, write}

object PubsubWriter {
  implicit val formats = DefaultFormats

  val topicName : ProjectTopicName = ProjectTopicName.of(OrderStreamer.projectID, OrderStreamer.pubTopicAddress);
  val publisher : Publisher  = Publisher.newBuilder(topicName).build();

  def pubBillTo(records: Array[BillToDim]) {
    // pub these
    for (record <- records) {
      val jsonString : String = write(record);
      val jsonData : ByteString = ByteString.copyFromUtf8(jsonString);
      try {
        val pubsubMessage : PubsubMessage = PubsubMessage.newBuilder().setData(jsonData).build();
        val messageIdFuture = publisher.publish(pubsubMessage);
      }
      catch {
        case e : Throwable => println(e);
      }
    }
  }

  def pubShipTo(records: Array[ShipToDim]) {
    for (record <- records) {
      val jsonString : String = write(record);
      val jsonData : ByteString = ByteString.copyFromUtf8(jsonString);
      try {
        val pubsubMessage : PubsubMessage = PubsubMessage.newBuilder().setData(jsonData).build();
        val messageIdFuture = publisher.publish(pubsubMessage);
      }
      catch {
        case e : Throwable => println(e);
      }
    }
  }
}