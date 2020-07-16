package streamer

import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import org.json4s.DefaultFormats
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.SaveMode
import java.text.SimpleDateFormat
import org.apache.spark.sql.functions._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import DatabaseWriter._

object DataTransformer {

  val dateFormatISO = new SimpleDateFormat("yyyy-MM-dd")

  def transformOrders(df: DataFrame, sc: SparkContext): Unit = {
    val sqlContext = SqlContextSingleton.getInstance(sc)
    import sqlContext.implicits._

    if (df.count > 0) {
      print(s"processing ${df.count} orders ...")
      print(" customers .")
      val dfCustomer = df
        .select(
          $"customer.email".alias("customer_email"),
          $"customer.id".alias("netsuite_id"),
          $"customer.name".alias("customer_name")
        )
        .distinct()
        .withColumn("customer_key", expr("uuid()"))

      // dfCustomer.createOrReplaceTempView("customers") // persist these in temp storage
      val dfCustomerResults = batchCustomerDim(dfCustomer.as[CustomerDim].collect()).toDF.distinct()

      val dfCustomerNew = dfCustomer
        .as("customer")
        .join(dfCustomerResults.as("keys"), $"customer.customer_key" === $"keys.old_key", "leftouter")
        .drop("customer_key", "old_key")
        .withColumnRenamed("new_key", "customer_key")
      print(" billto .")
      val dfBillTo = df
        .select(
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
        .distinct()
        .withColumn("billto_key", expr("uuid()"))
      val dfBillToResults = batchBilltoDim(dfBillTo.as[BillToDim].collect()).toDF.distinct()
      val dfBillToNew = dfBillTo
        .as("billto")
        .join(dfBillToResults.as("keys"), $"billto.billto_key" === $"keys.old_key", "leftouter")
        .drop("billto_key", "old_key")
        .withColumnRenamed("new_key", "billto_key")
      print(" shipto .")
      val dfShipTo = df
        .withColumn("shipments", explode($"shipments"))
        .distinct()
        .select(
          expr("uuid()").alias("shipto_key"), // generate shipto key
          $"shipments.addressKey".alias("netsuite_key"),
          $"shipments.name".alias("name"),
          $"shipments.addr1".alias("addr1"),
          $"shipments.addr2".alias("addr2"),
          $"shipments.city".alias("city"),
          $"shipments.state".alias("state"),
          $"shipments.zip".alias("zip"),
          $"shipments.phone".alias("phone"),
          $"shipments.type".alias("type")
        )
        .withColumn("country", lit("USA"))
        .withColumn("type", coalesce($"type", lit("Unassigned"))) // set to Unassigned if null
        .as("shipments")
        // lookup desttype key
        .join(
          OrderStreamer.dimDestTypes.as("desttype"),
          $"shipments.type" === $"desttype.desttype_name",
          "leftouter"
        )
        .drop("type", "desttype_name")
        .withColumn("desttype_key", coalesce($"desttype_key", lit(99))) // set to Unassigned if null
      val dfShipToResults = batchShiptoDim(dfShipTo.as[ShipToDim].collect()).toDF.distinct()
      val dfShipToNew = dfShipTo
        .as("shipto")
        .join(dfShipToResults.as("keys"), $"shipto.shipto_key" === $"keys.old_key", "leftouter")
        .drop("shipto_key", "old_key")
        .withColumnRenamed("new_key", "shipto_key")
      print(" order .")
      val dfOrders = df
        .select(
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
          $"totals.total".alias("total")
        )
        .withColumn("school_value", coalesce($"school_value", lit(0))) // set to 0 if null
        .withColumn("source_value", coalesce($"source_value", lit(0))) // set to 0 if null
        .withColumn("channel_value", coalesce($"channel_value", lit(0))) // set to 0 if null
        .withColumn("ocm_id", coalesce($"ocm_id", lit(0))) // set to 0 if null
        .withColumn("ocm_number", coalesce($"ocm_number", lit("N/A"))) // set to 0 if null
        .withColumn(
          "date_value",
          date_format($"date_value", "yyyy-MM-dd")
        ) // reformat the date value from date to a iso date string
        .as("orders")
        // look up SCD keys
        .join(
          OrderStreamer.dimSchools.as("schools"),
          $"orders.school_value" === $"schools.netsuite_id",
          "leftouter"
        )
        .drop("school_name", "school_code", "netsuite_id", "school_value")
        .join(OrderStreamer.dimDates.as("dates"), $"orders.date_value" === $"dates.date_string", "leftouter")
        .drop("date_string", "date_value")
        .join(
          OrderStreamer.dimSources.as("sources"),
          $"orders.source_value" === $"sources.netsuite_id",
          "leftouter"
        )
        .drop("source_name", "netsuite_id", "source_value")
        .join(
          OrderStreamer.dimChannels.as("channels"),
          $"orders.channel_value" === $"channels.netsuite_id",
          "leftouter"
        )
        .drop("channel_name", "netsuite_id", "channel_value")
        // look up customer and billto keys
        .join(dfCustomerNew.as("customers"), $"orders.customer_value" === $"customers.netsuite_id", "inner")
        .drop("customer_email", "netsuite_id", "customer_name", "customer_value")
        .join(dfBillToNew.as("billtos"), $"orders.billto_value" === $"billtos.netsuite_key", "inner")
        .drop("billto_value", "netsuite_key", "name", "addr1", "addr2", "city", "state", "zip", "country", "phone")
        .withColumnRenamed("netsuite_order_id", "netsuite_id")

      upsertOrdersFact(dfOrders.as[OrdersFact].collect())
      print(" lines .")
      val dfOrderLines = df
        .select(
          lit(0).alias("is_discount"), // fixed value for lines
          lit(0).alias("is_shipping"), // fixed value for lines
          lit(0).alias("is_service"), // fixed value for lines
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
        .withColumn("source_value", coalesce($"source_value", lit(0))) // set to 0 if null
        .withColumn("channel_value", coalesce($"channel_value", lit(0))) // set to 0 if null
        .withColumn("ocm_order_id", coalesce($"ocm_order_id", lit(0))) // set to 0 if null
        .withColumn("ocm_order_number", coalesce($"ocm_order_number", lit("N/A"))) // set to 0 if null
        .withColumn(
          "date_value",
          date_format($"date_value", "yyyy-MM-dd")
        ) // reformat the date value from date to a iso date string
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
        .withColumn("lob", coalesce($"lob", lit("Unassigned"))) // set to unassigned if null
        .drop("type", "unitPrice", "itemTitle", "itemSku") // drop these from lines
        .as("lines")
        // lookup keys
        .join(OrderStreamer.dimSchools.as("schools"), $"lines.school_value" === $"schools.netsuite_id", "leftouter")
        .drop("school_name", "school_code", "netsuite_id", "school_value")
        .join(OrderStreamer.dimDates.as("dates"), $"lines.date_value" === $"dates.date_string", "leftouter")
        .drop("date_string", "date_value")
        .join(OrderStreamer.dimSources.as("sources"), $"lines.source_value" === $"sources.netsuite_id", "leftouter")
        .drop("source_name", "netsuite_id", "source_value")
        .join(
          OrderStreamer.dimChannels.as("channels"),
          $"lines.channel_value" === $"channels.netsuite_id",
          "leftouter"
        )
        .drop("channel_name", "netsuite_id", "channel_value")
        .join(dfCustomerNew.as("customers"), $"lines.customer_value" === $"customers.netsuite_id", "inner")
        .drop("customer_email", "netsuite_id", "customer_name", "customer_value")
        .join(dfBillToNew.as("billtos"), $"lines.billto_value" === $"billtos.netsuite_key", "inner")
        .drop("billto_value", "netsuite_key", "name", "addr1", "addr2", "city", "state", "zip", "country", "phone")
        .join(dfShipToNew.as("shiptos"), $"lines.shipto_value" === $"shiptos.netsuite_key", "inner")
        .drop("shipto_value", "netsuite_key", "name", "addr1", "addr2", "city", "state", "zip", "type", "phone")
        .join(OrderStreamer.dimProducts.as("products"), $"lines.itemId" === $"products.netsuite_id", "leftouter")
        .drop("netsuite_id", "itemId", "sku", "title", "type", "lob_key", "avg_cost")
        .join(OrderStreamer.dimLobs.as("lobs"), $"lines.lob" === $"lobs.lob_name", "leftouter")
        .drop("lob_name", "lob")
        // rename columns to match
        .withColumnRenamed("extPrice", "total_price")
        .withColumnRenamed("cost", "total_cost")
        .withColumnRenamed("discount", "total_discount")
        .withColumnRenamed("shipping", "total_shipping")
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
        .withColumn("lob_key", coalesce($"lob_key", lit(99))) // set to unassigned if null
        .withColumn(
          "product_key",
          coalesce($"product_key", lit(72804))
        ) // set to fixed value of 72804 if we don't know what it is

      val dfOrderLineResults = batchOrderLinesFact(dfOrderLines.as[OrderLineFact].collect(), true).toDF
        .withColumnRenamed("cancelled", "prev_cancelled")
        .withColumnRenamed("price", "prev_price")
        .withColumnRenamed("tax", "prev_tax")
        .withColumnRenamed("cost", "prev_cost")
        .withColumnRenamed("discount", "prev_discount")
        .withColumnRenamed("shipping", "prev_shipping")
      print(" dsr .")
      val dfDSR = dfOrderLines
        .withColumnRenamed("date_key", "date_key_lines")
        .as("lines")
        // get the year
        .join(OrderStreamer.dimDates.as("dates"), $"lines.date_key_lines" === $"dates.date_key", "leftouter")
        .drop("date_key_lines")
        .withColumn("year", substring($"date_string", 0, 4))
        .drop("date_string")
        // look up schedule
        .join(OrderStreamer.dimSchedules.as("schedules"), $"year" === $"schedules.schedule_name", "leftouter")
        .drop("schedule_name", "year")
        // find out if this is new/existing and previously cancelled or not
        .join(dfOrderLineResults.as("updates"), $"lines.netsuite_line_id" === $"updates.line_id", "leftouter")
        .drop("line_id")
        // drop keys not needed for dsr
        .drop(
          "quantity",
          "shipment_number",
          "netsuite_line_id",
          "netsuite_line_key",
          "netsuite_order_id",
          "netsuite_order_number"
        )
        .drop(
          "ocm_order_id",
          "ocm_order_number",
          "school_key",
          "source_key",
          "customer_key",
          "billto_key",
          "shipto_key"
        )
        .drop("desttype_key", "product_key")

      // if not existing and not cancelled, add
      val dsr1 = dfDSR
        .where(dfDSR("existing") === false)
        .withColumn("total_price", when(col("is_cancelled") === 1, 0).otherwise(col("total_price")))
        .withColumn("total_cost", when(col("is_cancelled") === 1, 0).otherwise(col("total_cost")))
        .withColumn("total_tax", when(col("is_cancelled") === 1, 0).otherwise(col("total_tax")))
        .groupBy("schedule_key", "date_key", "channel_key", "lob_key", "is_dropship")
        .agg(
          sum("total_price").as("price"),
          sum("total_cost").as("cost"),
          sum("total_tax").as("tax"),
          sum("total_discount").as("discount"),
          sum("total_shipping").as("shipping")
        )

      // if existing, and not cancelled ever, add the difference
      var dsr2 = dfDSR
        .where(dfDSR("existing") === true && dfDSR("is_cancelled") === 0 && dfDSR("prev_cancelled") === false)
        .withColumn("total_price", col("total_price") - col("prev_price"))
        .withColumn("total_cost", col("total_cost") - col("prev_cost"))
        .withColumn("total_tax", col("total_tax") - col("prev_tax"))
        .groupBy("schedule_key", "date_key", "channel_key", "lob_key", "is_dropship")
        .agg(
          sum("total_price").as("price"),
          sum("total_cost").as("cost"),
          sum("total_tax").as("tax"),
          sum("total_discount").as("discount"),
          sum("total_shipping").as("shipping")
        )

      // if existing and uncancelled
      var dsr3 = dfDSR
        .where(dfDSR("existing") === true && dfDSR("is_cancelled") === 0 && dfDSR("prev_cancelled") === true)
        .withColumn("total_price", col("total_price"))
        .withColumn("total_cost", col("total_cost"))
        .withColumn("total_tax", col("total_tax"))
        .groupBy("schedule_key", "date_key", "channel_key", "lob_key", "is_dropship")
        .agg(
          sum("total_price").as("price"),
          sum("total_cost").as("cost"),
          sum("total_tax").as("tax"),
          sum("total_discount").as("discount"),
          sum("total_shipping").as("shipping")
        )

      // if existing and now cancelled, subtract
      var dsr4 = dfDSR
        .where(dfDSR("existing") === true && dfDSR("is_cancelled") === 1 && dfDSR("prev_cancelled") === false)
        .withColumn("total_price", -col("prev_price"))
        .withColumn("total_cost", -col("prev_cost"))
        .withColumn("total_tax", -col("prev_tax"))
        .groupBy("schedule_key", "date_key", "channel_key", "lob_key", "is_dropship")
        .agg(
          sum("total_price").as("price"),
          sum("total_cost").as("cost"),
          sum("total_tax").as("tax"),
          sum("total_discount").as("discount"),
          sum("total_shipping").as("shipping")
        )
      var dsr = dsr1
        .union(dsr2)
        .union(dsr3)
        .union(dsr4)

      upsertDSRFact(dsr.as[DailySalesFact].collect())
      print(" fee .")
      // process the fees
      val dfOrderFees = df
        .select(
          lit(0).alias("is_discount"), // fixed value for lines
          lit(0).alias("is_shipping"), // fixed value for lines
          lit(0).alias("is_service"), // fixed value for lines
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
          $"fees"
        )
        .withColumn("school_value", coalesce($"school_value", lit(0))) // set to 0 if null
        .withColumn("source_value", coalesce($"source_value", lit(0))) // set to 0 if null
        .withColumn("channel_value", coalesce($"channel_value", lit(0))) // set to 0 if null
        .withColumn("ocm_order_id", coalesce($"ocm_order_id", lit(0))) // set to 0 if null
        .withColumn("ocm_order_number", coalesce($"ocm_order_number", lit("N/A"))) // set to 0 if null
        .withColumn(
          "date_value",
          date_format($"date_value", "yyyy-MM-dd")
        ) // reformat the date value from date to a iso date string
        // expand nested shipments
        .withColumn("fees", explode($"fees"))
        .select(
          "fees.*",
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
        .withColumn("lob", coalesce($"lob", lit("Unassigned"))) // set to unassigned if null
        .drop("type", "unitPrice", "itemTitle", "itemSku") // drop these from lines
        .as("lines")
        // lookup keys
        .join(OrderStreamer.dimSchools.as("schools"), $"lines.school_value" === $"schools.netsuite_id", "leftouter")
        .drop("school_name", "school_code", "netsuite_id", "school_value")
        .join(OrderStreamer.dimDates.as("dates"), $"lines.date_value" === $"dates.date_string", "leftouter")
        .drop("date_string", "date_value")
        .join(OrderStreamer.dimSources.as("sources"), $"lines.source_value" === $"sources.netsuite_id", "leftouter")
        .drop("source_name", "netsuite_id", "source_value")
        .join(
          OrderStreamer.dimChannels.as("channels"),
          $"lines.channel_value" === $"channels.netsuite_id",
          "leftouter"
        )
        .drop("channel_name", "netsuite_id", "channel_value")
        .join(dfCustomerNew.as("customers"), $"lines.customer_value" === $"customers.netsuite_id", "inner")
        .drop("customer_email", "netsuite_id", "customer_name", "customer_value")
        .join(dfBillToNew.as("billtos"), $"lines.billto_value" === $"billtos.netsuite_key", "inner")
        .drop("billto_value", "netsuite_key", "name", "addr1", "addr2", "city", "state", "zip", "country", "phone")
        .join(OrderStreamer.dimProducts.as("products"), $"lines.itemId" === $"products.netsuite_id", "leftouter")
        .drop("netsuite_id", "itemId", "sku", "title", "type", "lob_key", "avg_cost")
        .join(OrderStreamer.dimLobs.as("lobs"), $"lines.lob" === $"lobs.lob_name", "leftouter")
        .drop("lob_name", "lob")
        // rename columns to match
        .withColumnRenamed("extPrice", "total_price")
        .withColumnRenamed("cost", "total_cost")
        .withColumnRenamed("discount", "total_discount")
        .withColumnRenamed("shipping", "total_shipping")
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
        .withColumn("lob_key", coalesce($"lob_key", lit(99))) // set to unassigned if null
        .withColumn(
          "product_key",
          coalesce($"product_key", lit(72804))
        ) // set to fixed value of 72804 if we don't know what it is
        .withColumn("shipto_key", lit("00000000-0000-0000-0000-000000000000"))
        .withColumn("desttype_key", lit(99))

      batchOrderLinesFact(dfOrderFees.as[OrderLineFact].collect(), false)
      println("done")
    }
  }
}