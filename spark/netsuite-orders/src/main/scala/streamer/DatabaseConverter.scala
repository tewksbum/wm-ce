package streamer

import java.sql.{Connection, DriverManager}
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext._
import scala.collection.mutable.ListBuffer

object DatabaseConverter {

  def getConnection(): java.sql.Connection = {
    var connection = DriverManager.getConnection(OrderStreamer.jdbcMysqlUrl, OrderStreamer.jdbcUserName, OrderStreamer.jdbcPassword)
    connection
  }

  def upsertCustomerDim(
      records: Array[streamer.CustomerDim]
  ): List[PeopleUpsertResult] = {
    var results = new ListBuffer[PeopleUpsertResult]()
    if (records.length > 0) {
      val con: java.sql.Connection = getConnection
      val ps = con.prepareCall("{call sp_upsert_customer (?,?,?,?)}")
      println("upsert customer")
      for (record <- records) {
        ps.clearParameters()
        ps.setString(1, record.customer_key)
        ps.setString(2, record.customer_name)
        ps.setString(3, record.customer_email)
        ps.setLong(4, record.netsuite_id)
        val rs = ps.executeQuery()
        if (rs.next()) {
          val result = new PeopleUpsertResult(
            rs.getString(1),
            rs.getString(2)
          )
          results += result
        }
      }
      ps.close
      con.close
    }
    results.toList
  }

  def upsertBilltoDim(
      records: Array[streamer.BillToDim]
  ): List[PeopleUpsertResult] = {
    var results = new ListBuffer[PeopleUpsertResult]()
    if (records.length > 0) {
      val con: java.sql.Connection = getConnection
      val ps =
        con.prepareCall("{call sp_upsert_billtos (?,?,?,?,?, ?,?,?,?,?)}")
      println("upsert billto")
      for (record <- records) {
        ps.clearParameters()
        ps.setString(1, record.billto_key)
        ps.setString(2, record.netsuite_key)
        ps.setString(3, record.name)
        ps.setString(4, record.addr1)
        ps.setString(5, record.addr2)
        ps.setString(6, record.city)
        ps.setString(7, record.state)
        ps.setString(8, record.zip)
        ps.setString(9, record.country)
        ps.setString(10, record.phone)
        val rs = ps.executeQuery()
        if (rs.next()) {
          val result = new PeopleUpsertResult(
            rs.getString(1),
            rs.getString(2)
          )
          results += result
        }
      }
      ps.close
      con.close
    }
    results.toList
  }

  def upsertShiptoDim(
      records: Array[streamer.ShipToDim]
  ): List[PeopleUpsertResult] = {
    var results = new ListBuffer[PeopleUpsertResult]()
    if (records.length > 0) {
      val con: java.sql.Connection = getConnection
      val ps =
        con.prepareCall("{call sp_upsert_shiptos (?,?,?,?,?, ?,?,?,?,?, ?)}")
      println("upsert shipto")
      for (record <- records) {
        ps.clearParameters()
        ps.setString(1, record.shipto_key)
        ps.setString(2, record.netsuite_key)
        ps.setString(3, record.name)
        ps.setString(4, record.addr1)
        ps.setString(5, record.addr2)
        ps.setString(6, record.city)
        ps.setString(7, record.state)
        ps.setString(8, record.zip)
        ps.setString(9, record.country)
        ps.setString(10, record.phone)
        ps.setLong(11, record.desttype_key)
        val rs = ps.executeQuery()
        if (rs.next()) {
          val result = new PeopleUpsertResult(
            rs.getString(1),
            rs.getString(2)
          )
          results += result
        }
      }
      ps.close
      con.close
    }
    results.toList
  }

  def upsertDSRFact(records: Array[streamer.DailySalesFact]): Unit = {
    if (records.length > 0) {
      val con: java.sql.Connection = getConnection
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
      con.close
    }
  }

  def upsertOrdersFact(records: Array[streamer.OrdersFact]): Unit = {
    if (records.length > 0) {
      val con: java.sql.Connection = getConnection
      val ps = con.prepareCall(
        "{call sp_upsert_orders (?,?,?,?,?,?,  ?,?,?,?,?,?,?,  ?,?,?,?,?,?)}"
      )
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
      con.close
    }
  }

  def upsertOrderLinesFact(
      records: Array[streamer.OrderLineFact]
  ): List[LineUpsertResult] = {
    var results = new ListBuffer[LineUpsertResult]()
    if (records.length > 0) {
      val con: java.sql.Connection = getConnection
      val ps = con.prepareCall(
        "{call sp_upsert_orderlines (?,?,?,?,?,?,?,?,  ?,?,?,?,?,?,?,?,?,?,  ?,?,?,?,?,?,?,?)}"
      )
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

        val rs = ps.executeQuery()
        // this query returns
        // select 1 as existing, is_cancelled as cancelled, total_price as price, total_tax as tax, total_cost as cost

        if (rs.next()) {
          val result = new LineUpsertResult(
            rs.getString(1),
            rs.getBoolean(2),
            rs.getBoolean(3),
            rs.getDouble(4),
            rs.getDouble(5),
            rs.getDouble(6)
          )
          results += result
        }
      }
      ps.close
      con.close
    }
    results.toList
  }

}
