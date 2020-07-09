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
import DatabaseConverter._

object OrderProcessor {

  implicit val formats = DefaultFormats

  val dateFormatISO = new SimpleDateFormat("yyyy-MM-dd")

  // [START extract]
  def extractNetsuiteOrder(input: RDD[String]): RDD[NetsuiteOrder] = {
    input.collect().foreach(println)
    return input.map( x => parse(x).extract[NetsuiteOrder])
  }

  def processOrders(input: DStream[String], windowLength: Int, slidingInterval: Int): Unit = {
    val orders: DStream[NetsuiteOrder] = input
      .window(Seconds(windowLength), Seconds(slidingInterval)) //create a window
      .transform(extractNetsuiteOrder(_)) //apply transformation
    
    orders.foreachRDD(rdd => {
      val sqlContext = SqlContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._

      if (rdd.count > 0) {
        println(s"processing ${rdd.count} orders\n")
        val df = rdd.toDF() // data frame

        // note spark sql write.jdbc will double quote the column names, this is not acceptable to mariadb ootb
        // to update it, start by running this sql
        // `select @@SQL_MODE`
        // then append the value ',ANSI_QUOTES' to the value and run a set
        // `SET GLOBAL sql_mode ='STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION,ANSI_QUOTES'`
        // the 3 dims are written with write.jdbc

        // map to the dims and facts that need to be populated
        val dfCustomer = df.select(
          lit(randomUUID().toString).alias("customer_key"), //generate a customer key
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
          $"id".alias("netsuite_order_id"),
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
        .withColumn("school_value", coalesce($"school_value", lit(0))) // set to 0 if null
        .withColumn("date_value", date_format($"date_value","yyyy-MM-dd"))  // reformat the date value from date to a iso date string
        .as("orders")
        // look up SCD keys
        .join(OrderStreamer.dimSchools.as("schools"), $"orders.school_value" === $"schools.netsuite_id", "leftouter").drop("school_name", "school_code", "netsuite_id", "school_value")
        .join(OrderStreamer.dimDates.as("dates"), $"orders.date_value" === $"dates.date_string", "leftouter").drop("date_string", "date_value")
        .join(OrderStreamer.dimSources.as("sources"), $"orders.source_value" === $"sources.netsuite_id", "leftouter").drop("source_name", "netsuite_id", "source_value")
        .join(OrderStreamer.dimChannels.as("channels"), $"orders.channel_value" === $"channels.netsuite_id", "leftouter").drop("channel_name", "netsuite_id", "channel_value")
        // look up customer and billto keys
        .join(dfCustomer.as("customers"), $"orders.customer_value" === $"customers.netsuite_id", "inner").drop("customer_email", "netsuite_id", "customer_name", "customer_value")
        .join(dfBillTo.as("billtos"), $"orders.billto_value" === $"billtos.netsuite_key", "inner").drop("billto_value", "netsuite_key", "name", "addr1", "addr2", "city", "state", "zip", "country", "phone")
        .withColumnRenamed("netsuite_order_id", "netsuite_id")
        dfOrders.show()
        upsertOrdersFact(dfOrders.as[OrdersFact].collect())
        // dfOrders.write.mode(SaveMode.Append).jdbc(OrderStreamer.jdbcUrl, "fact_orders", OrderStreamer.jdbcProperties)

        // map to orderlines fact
        val dfShipTos = df
        .withColumn("shipments", explode($"shipments"))
        .select(
          lit(randomUUID().toString).alias("shipto_key"), // generate shipto key
          $"shipments.addressKey".alias("netsuite_key"),
          $"shipments.name".alias("name"),
          $"shipments.addr1".alias("addr1"),
          $"shipments.addr2".alias("addr2"),
          $"shipments.city".alias("city"),
          $"shipments.state".alias("state"),
          $"shipments.zip".alias("zip"),
          $"shipments.phone".alias("phone"),
          $"shipments.type".alias("type"),
        )
        .as("shipments")
        // lookup desttype key
        .join(OrderStreamer.dimDestTypes.as("desttype"), $"shipments.type" === $"desttype.desttype_name", "leftouter").drop("type", "desttype_name")
        dfShipTos.show()
        dfShipTos.write.mode(SaveMode.Append).jdbc(OrderStreamer.jdbcUrl, "dim_shiptos", OrderStreamer.jdbcProperties)

        val dfOrderLines = df
        .select(
          lit(0).alias("is_discount"),  // fixed value for lines
          lit(0).alias("is_shipping"),  // fixed value for lines
          lit(0).alias("is_service"),  // fixed value for lines
          $"dates.placedOn".alias("date_value"), 
          $"attributes.channelId".alias("channel_value"), 
          $"attributes.sourceId".alias("source_value"), 
          $"attributes.schoolId".alias("school_value"), 
          $"customer.id".alias("customer_value"),
          $"billing.addressKey".alias("billto_value"),
          $"id".alias("netsuite_order_id"),
          $"orderNumber".alias("netsuite_order_number"), 
          $"attributes.webOrderId".alias("ocm_order_id"),
          $"attributes.webOrderNumber".alias("ocm_order_number"),           
          $"shipments"
        )
        .withColumn("school_value", coalesce($"school_value", lit(0))) // set to 0 if null
        .withColumn("date_value", date_format($"date_value","yyyy-MM-dd"))  // reformat the date value from date to a iso date string
        // expand nested shipments
        .withColumn("shipments", explode($"shipments"))
        .select(
          "shipments.*",
          "is_discount",
          "is_shipping",
          "is_service",
          "date_value",
          "channel_value",
          "source_value",
          "school_value",
          "customer_value",
          "billto_value",
          "netsuite_order_id",
          "netsuite_order_number",
          "ocm_order_id",
          "ocm_order_number"
        )
        // remove shipment fields that we dont need
        .drop("addr1", "addr2", "city", "state", "zip", "name", "phone", "email", "type")
        .withColumnRenamed("addressKey", "shipto_value")
        // expand lines
        .withColumn("lines", explode($"lines"))
        .select(
          "lines.*",
          "is_discount",
          "is_shipping",
          "is_service",
          "shipto_value",
          "date_value",
          "channel_value",
          "source_value",
          "school_value",
          "customer_value",
          "billto_value",
          "netsuite_order_id",
          "netsuite_order_number",
          "ocm_order_id",
          "ocm_order_number"
        )
        .drop("type", "unitPrice", "itemTitle", "itemSku") // drop these from lines
        .as("lines")
        // lookup keys
        .join(OrderStreamer.dimSchools.as("schools"), $"lines.school_value" === $"schools.netsuite_id", "leftouter").drop("school_name", "school_code", "netsuite_id", "school_value")
        .join(OrderStreamer.dimDates.as("dates"), $"lines.date_value" === $"dates.date_string", "leftouter").drop("date_string", "date_value")
        .join(OrderStreamer.dimSources.as("sources"), $"lines.source_value" === $"sources.netsuite_id", "leftouter").drop("source_name", "netsuite_id", "source_value")
        .join(OrderStreamer.dimChannels.as("channels"), $"lines.channel_value" === $"channels.netsuite_id", "leftouter").drop("channel_name", "netsuite_id", "channel_value")
        .join(dfCustomer.as("customers"), $"lines.customer_value" === $"customers.netsuite_id", "inner").drop("customer_email", "netsuite_id", "customer_name", "customer_value")
        .join(dfBillTo.as("billtos"), $"lines.billto_value" === $"billtos.netsuite_key", "inner").drop("billto_value", "netsuite_key", "name", "addr1", "addr2", "city", "state", "zip", "country", "phone")
        .join(dfShipTos.as("shiptos"), $"lines.shipto_value" === $"shiptos.netsuite_key", "inner").drop("shipto_value", "netsuite_key", "name", "addr1", "addr2", "city", "state", "zip", "type", "phone")
        .join(OrderStreamer.dimProducts.as("products"), $"lines.itemId" === $"products.netsuite_id", "leftouter").drop("netsuite_id", "itemId", "sku", "title", "type", "lob_key", "avg_cost")
        .join(OrderStreamer.dimLobs.as("lobs"), $"lines.lob" === $"lobs.lob_name", "leftouter").drop("lob_name", "lob")
        // rename columns to match
        .withColumnRenamed("extPrice", "total_price")
        .withColumnRenamed("cost", "total_cost")
        .withColumnRenamed("shipment", "shipment_number")
        .withColumnRenamed("isDropship", "is_dropship")
        .withColumn("is_dropship", when(col("is_dropship") === true, 1).otherwise(0))
        .withColumnRenamed("isCancelled", "is_cancelled")
        .withColumn("is_cancelled", when(col("is_cancelled") === true, 1).otherwise(0))
        .withColumnRenamed("tax", "total_tax")
        .withColumnRenamed("lineId", "netsuite_line_id")
        .withColumnRenamed("uniqueKey", "netsuite_line_key")
        // replace null values
        .withColumn("quantity", coalesce($"quantity", lit(1))) // set to 1 if null
        .withColumn("total_cost", coalesce($"total_cost", lit(0))) // set to 1 if null
        dfOrderLines.show()
        upsertOrderLinesFact(dfOrderLines.as[OrderLineFact].collect())
        // dfOrderLines.write.mode(SaveMode.Append).jdbc(OrderStreamer.jdbcUrl, "fact_orderlines", OrderStreamer.jdbcProperties)

        // map to dsr fact
        val dfDSR = dfOrderLines
        .withColumnRenamed("date_key", "date_key_lines")
        .as("lines")
        // get the year
        .join(OrderStreamer.dimDates.as("dates"), $"lines.date_key_lines" === $"dates.date_key", "leftouter").drop("date_key_lines")
        .withColumn("year", substring($"date_string", 0, 4)).drop("date_string")
        // look up schedule
        .join(OrderStreamer.dimSchedules.as("schedules"), $"year" === $"schedules.schedule_name", "leftouter").drop("schedule_name", "year")
        // drop keys not needed for dsr
        .drop("quantity", "shipment_number", "netsuite_line_id", "netsuite_line_key", "netsuite_order_id", "netsuite_order_number")
        .drop("ocm_order_id", "ocm_order_number", "school_key", "source_key", "customer_key", "billto_key", "shipto_key")
        .drop("desttype_key", "product_key")
        // aggregate only if not cancelled
        .withColumn("total_price", when(col("is_cancelled") === 1, 0).otherwise(col("total_price")))
        .withColumn("total_cost", when(col("is_cancelled") === 1, 0).otherwise(col("total_cost")))
        .withColumn("total_tax", when(col("is_cancelled") === 1, 0).otherwise(col("total_tax")))
        // group by keys and aggregate
        .groupBy("schedule_key", "date_key", "channel_key", "lob_key", "is_dropship")
        .agg(
          sum("total_price").as("price"),
          sum("total_cost").as("cost"),
          sum("total_tax").as("tax")
        )
        dfDSR.show()
        upsertDSRFact(dfDSR.as[DailySalesFact].collect())
      }
    })
  }
}
