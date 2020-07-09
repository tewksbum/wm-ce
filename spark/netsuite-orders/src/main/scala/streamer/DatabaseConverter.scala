package streamer

import java.sql.{Connection,DriverManager}
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext._

object DatabaseConverter {

  def getConnection(jdbcUrl: String) : java.sql.Connection = {
    var connection = DriverManager.getConnection(jdbcUrl)
    connection
  }

  def upsertDSRFact(records: Array[streamer.DailySalesFact]): Unit = {
    if (records.length > 0) {
      val con : java.sql.Connection = getConnection(OrderStreamer.jdbcUrl)
      val ps = con.prepareCall("{call sp_upsert_dsr (?,?,?,?,?,?,?,?)}")
      println("upsert dsr")
      for (record <- records) {
        ps.clearParameters()
        ps.setLong(1, record.schedule_key)
        ps.setLong(2, record.date_key)
        ps.setLong(3, record.channel_key)
        ps.setLong(4, record.lob_key)
        ps.setInt(5, record.is_dropship)
        ps.setDouble(6, record.price)
        ps.setDouble(7, record.cost)
        ps.setDouble(8, record.tax)
        ps.executeUpdate() 
      }
      ps.close
    }
  }

  def upsertOrdersFact(records: Array[streamer.OrdersFact]): Unit = {
    if (records.length > 0) {
      val con : java.sql.Connection = getConnection(OrderStreamer.jdbcUrl)
      val ps = con.prepareCall("{call sp_upsert_orders (?,?,?,?,?,?,  ?,?,?,?,?,?,?,  ?,?,?,?,?,?)}")
      println("upsert orders")
      for (record <- records) {
        ps.setLong(1, record.date_key)
        ps.setLong(2, record.channel_key)
        ps.setLong(3, record.source_key)
        ps.setLong(4, record.school_key)
 				ps.setString(5, record.customer_key)
        ps.setString(6, record.billto_key)
        ps.setLong(7, record.netsuite_id)
        ps.setString(8, record.netsuite_number)
        ps.setLong(9, record.ocm_id)
        ps.setString(10, record.ocm_number)
        ps.setDouble(11, record.merchandise_cost)
        ps.setDouble(12, record.merchandise_total)
        ps.setDouble(13, record.merchandise_tax)
        ps.setDouble(14, record.shipping)
        ps.setDouble(15, record.shipping_tax)
        ps.setDouble(16, record.discount)
        ps.setDouble(17, record.service)
        ps.setDouble(18, record.service_tax)
        ps.setDouble(19, record.total)
        ps.executeUpdate() 
      }
      ps.close
    }
  }

  def upsertOrderLinesFact(records: Array[streamer.OrderLineFact]): Unit = {
    if (records.length > 0) {
      val con : java.sql.Connection = getConnection(OrderStreamer.jdbcUrl)
      val ps = con.prepareCall("{call sp_upsert_orderlines (?,?,?,?,?,?,?,?,  ?,?,?,?,?,?,?,?,?,?,  ?,?,?,?,?,?,?,?)}")
      println("upsert lines")
      for (record <- records) {
        ps.setLong(1, record.date_key)
        ps.setLong(2, record.channel_key)
        ps.setLong(3, record.source_key)
        ps.setLong(4, record.school_key)
 				ps.setString(5, record.customer_key)
        ps.setLong(6, record.product_key)
        ps.setString(7, record.billto_key)
        ps.setString(8, record.shipto_key)
        ps.setLong(9, record.netsuite_order_id)
        ps.setString(10, record.netsuite_order_number)
        ps.setLong(11, record.ocm_order_id)
        ps.setString(12, record.ocm_order_number)
        ps.setString(13, record.shipment_number)
        ps.setString(14, record.netsuite_line_id)
        ps.setString(15, record.netsuite_line_key)
        ps.setLong(16, record.lob_key)
        ps.setLong(17, record.desttype_key)
        ps.setInt(18, record.is_dropship)
        ps.setDouble(19, record.total_price)
        ps.setDouble(20, record.total_tax)
        ps.setDouble(21, record.total_cost)
 				ps.setInt(22, record.quantity)
  			ps.setInt(23, record.is_discount)
 				ps.setInt(24, record.is_shipping)
  			ps.setInt(25, record.is_service)
  			ps.setInt(26, record.is_cancelled)

        ps.executeUpdate() 
      }
      ps.close
    }
  }

}
