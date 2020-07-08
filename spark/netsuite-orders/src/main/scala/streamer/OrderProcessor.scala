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
import org.apache.spark.sql.functions.{current_date, current_timestamp, date_format}

object OrderProcessor {

  implicit val formats = DefaultFormats

  val dateFormatISO = new SimpleDateFormat("yyyy-MM-dd")

  // [START extract]
  def extractNetsuiteOrder(input: RDD[String]): RDD[NetsuiteOrder] = input.map( x => parse(x).extract[NetsuiteOrder])

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

      if (rdd.count > 0) {
        val df = rdd.toDF() // data frame

        // note spark sql jdbc write will double quote the column names, this is not acceptable to mariadb ootb
        // to update it, start by running this sql
        // `select @@SQL_MODE`
        // then append the value ',ANSI_QUOTES' to the value and run a set
        // `SET GLOBAL sql_mode ='STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION,ANSI_QUOTES'`

        // map to the dims and facts that need to be populated
        val dfCustomer = df.select(
          lit(randomUUID().toString).alias("customer_key"),
          $"customer.email".alias("customer_email"),
          $"customer.id".alias("netsuite_id"),
          $"customer.name".alias("customer_name")
        )
        dfCustomer.createOrReplaceTempView("customers") // persist these in temp storage
        // TODO: look up existing customer
        // val existingCustomer = sqlContext.read.jdbc(jdbcUrl, "(select customer_key, netsuite_id from dim_customers where ) existing", jdbcProperties)
        // TODO: filter on new customer
        dfCustomer.write.mode(SaveMode.Append).jdbc(OrderStreamer.jdbcUrl, "dim_customers", OrderStreamer.jdbcProperties)

        val dfBillTo = df.select(
          lit(randomUUID().toString).alias("billto_key"),
          $"billing.addressKey".alias("netsuite_key"),
          $"billing.name",
          $"billing.addr1",
          $"billing.addr2",
          $"billing.city",
          $"billing.state",
          $"billing.zip",
          $"billing.country",
          $"billing.phone"
        )
        dfBillTo.createOrReplaceTempView("billtos")
        dfBillTo.write.mode(SaveMode.Append).jdbc(OrderStreamer.jdbcUrl, "dim_billtos", OrderStreamer.jdbcProperties)

        val dfOrders = df.select(
          $"dates.placedOn".alias("date_value"), 
          $"attributes.channelId".alias("channel_value"), 
          $"attributes.sourceId".alias("source_value"), 
          $"attributes.schoolId".alias("school_value"), 
          $"customer.id".alias("customer_value"),
          $"billing.addressKey".alias("billto_value"),
          $"id".alias("netsuite_id"),
          $"orderNumber".alias("netsuite_number"), 
          $"attributes.webOrderId".alias("ocm_id"),
          $"attributes.webOrderNumber".alias("ocm_number"), 
          $"totals.merchandiseCostTotal".alias("merchandise_cost"),
          $"totals.merchandiseTotal".alias("merchandise_total"),
          $"totals.merchandiseTaxTotal".alias("merchandise_tax"),
          $"totals.shippingTotal".alias("shipping"),
          $"totals.shippingTaxTotal".alias("shipping_tax"),
          $"totals.discountTotal".alias("discount"),
          $"totals.serviceTotal".alias("service"),
          $"totals.serviceTaxTotal".alias("service_tax"),
          $"totals.total".alias("total"),
        )
        .withColumn("date_value", date_format($"date_value","yyyy-MM-dd"))  // reformat the date value from date to a iso date string
        .as("orders")
        // look up SCD keys
        .join(OrderStreamer.dimSchools.as("schools"), $"orders.school_value" === $"schools.netsuite_id", "leftouter").drop("school_name", "school_code", "netsuite_id", "school_value")
        .join(OrderStreamer.dimDates.as("dates"), $"orders.date_value" === $"dates.date_string", "leftouter").drop("date_string", "date_value")
        .join(OrderStreamer.dimSources.as("sources"), $"orders.source_value" === $"sources.netsuite_id", "leftouter").drop("source_name", "netsuite_id", "source_value")
        .join(OrderStreamer.dimChannels.as("channels"), $"orders.channel_value" === $"channels.netsuite_id", "leftouter").drop("channel_name", "netsuite_id", "channel_value")
        .join(dfCustomer.as("customers"), $"orders.customer_value" === $"customers.netsuite_id", "inner").drop("customer_email", "netsuite_id", "customer_name", "customer_value")
        .join(dfBillTo.as("billtos"), $"orders.billto_value" === $"billtos.netsuite_key", "inner").drop("billto_value", "netsuite_key", "name", "addr1", "addr2", "city", "state", "zip", "country", "phone")
        // when($"F3" > 3, $"F4").otherwise(0.0) // conditional
        dfOrders.show()
        dfOrders.write.mode(SaveMode.Append).jdbc(OrderStreamer.jdbcUrl, "fact_orders", OrderStreamer.jdbcProperties)

        // array_distinct
        // map to orderlines fact

        // map to dsr fact

        val ds:Dataset[NetsuiteOrder] = df.as[NetsuiteOrder] // dataset

        // df.write.mode(SaveMode.Append).jdbc(OrderStreamer.jdbcUrl, "intake_data_dump", OrderStreamer.jdbcProperties)
      }
      handler(rdd.collect()) //take top N hashtags and save to external source
    })
  }
}
