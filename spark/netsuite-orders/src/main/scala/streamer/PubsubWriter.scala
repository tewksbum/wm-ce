package streamer

import com.github.hyjay.pubsub.Publisher
import cats.effect.IO
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.DefaultFormats
import org.json4s.native.Serialization.{read, write}

object PubsubWriter {
  implicit val formats = DefaultFormats

  //val topicName : ProjectTopicName = ProjectTopicName.of(OrderStreamer.projectID, OrderStreamer.pubTopicAddress);
  // val publisher   = Publisher.create(OrderStreamer.projectID, OrderStreamer.pubTopicAddress);

  // owner = sponsor, source = order.channel, event type = order, event = orderId
  def pubBillTo(records: Array[BillToDim]) {
    // pub these
    for (record <- records) {
      val jsonString : String = write(record);
      val io = for {
        publisher <- Publisher.create(OrderStreamer.projectID, OrderStreamer.pubTopicAddress)
        messageId <- publisher.publish(jsonString.getBytes())
      } yield()
      io.unsafeRunSync()
      //publisher.publish(jsonString.getBytes());
    }
  }

  def pubShipTo(records: Array[ShipToDim]) {
    for (record <- records) {
      val jsonString : String = write(record);
      val io = for {
        publisher <- Publisher.create(OrderStreamer.projectID, OrderStreamer.pubTopicAddress);
        messageId <- publisher.publish(jsonString.getBytes())
      } yield()
      io.unsafeRunSync()
    }
  }
}