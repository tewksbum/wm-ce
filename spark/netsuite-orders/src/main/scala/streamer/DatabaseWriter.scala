package streamer

import java.sql.{Connection, DriverManager, ResultSet}
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext._
import scala.collection.mutable.ListBuffer
import scalikejdbc._

object DatabaseWriter {

  // default connections
  def getConnection(): java.sql.Connection = {
    var connection =
      DriverManager.getConnection(OrderStreamer.jdbcMysqlUrl, OrderStreamer.jdbcUserName, OrderStreamer.jdbcPassword)
    connection
  }

  // scalalikejdbc connection pool
  Class.forName("com.mysql.cj.jdbc.Driver")
  // Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
  // Class.forName("net.sourceforge.jtds.jdbc.Driver");
  ConnectionPool.singleton(OrderStreamer.jdbcMysqlUrl, OrderStreamer.jdbcUserName, OrderStreamer.jdbcPassword)

  def upsertCustomerDim(
      records: Array[streamer.CustomerDim]
  ): List[PeopleUpsertResult] = {
    var results = new ListBuffer[PeopleUpsertResult]()
    if (records.length > 0) {
      using(ConnectionPool.borrow()) { con =>
        {
          val ps = con.prepareCall("{call sp_upsert_customer (?,?,?,?)}")
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
        }
      }
    }
    results.toList
  }

  def batchCustomerDim(records: Array[CustomerDim]) : List[PeopleUpsertResult] = {
    var results = new ListBuffer[PeopleUpsertResult]()
    if (records.length > 0) {
      using(ConnectionPool.borrow()) { con =>
        {
          val id = java.util.UUID.randomUUID.toString.replace('-', '_') 
          val st = con.createStatement
          val tbl = st.executeUpdate(s"create temporary table dim_customers_${id} like dim_customers ")
          val ps = con.prepareStatement(s"insert into dim_customers_${id} (customer_key,customer_name,customer_email,netsuite_id) values(?,?,?,?)")
          for (record <- records) {
            ps.setString(1, record.customer_key)
            ps.setString(2, record.customer_name)
            ps.setString(3, record.customer_email)
            ps.setLong(4, record.netsuite_id)
            ps.addBatch
          }
          ps.executeBatch
          ps.close
          // run a select to get the result, followed by insert and update
          val sqlSelect = s"""
            select a.customer_key as old_key, b.customer_key as new_key 
            from dim_customers_${id} a, dim_customers b 
            where a.netsuite_id = b.netsuite_id 
            union all 
            select a.customer_key, a.customer_key 
            from dim_customers_${id} a 
            where not exists (select 1 from dim_customers b where b.netsuite_id = a.netsuite_id)
          """
          val sqlInsert = s"""
            insert into dim_customers (customer_key, customer_name, customer_email, netsuite_id)
            select customer_key, customer_name, customer_email, netsuite_id 
            from dim_customers_${id} a 
            where not exists (select 1 from dim_customers b where b.netsuite_id = a.netsuite_id) 
          """
          val sqlUpdate = s"""
            update dim_customers b
            inner join dim_customers_${id}  a
            on (a.netsuite_id = b.netsuite_id)
            set b.customer_name = a.customer_name, b.customer_email = a.customer_email
          """

          val rs: ResultSet = st.executeQuery(sqlSelect)
          while (rs.next) {
            results += new PeopleUpsertResult(
              rs.getString(1),
              rs.getString(2)
            )
          }
          rs.close
          st.executeUpdate(sqlUpdate)
          st.executeUpdate(sqlInsert)
          st.close
        }
      }
    }
    results.toList
  }

  def upsertBilltoDim(
      records: Array[BillToDim]
  ): List[PeopleUpsertResult] = {
    var results = new ListBuffer[PeopleUpsertResult]()
    if (records.length > 0) {
      using(ConnectionPool.borrow()) { con =>
        {
          val ps = con.prepareCall("{call sp_upsert_billtos (?,?,?,?,?, ?,?,?,?,?)}")
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
        }
      }
    }
    results.toList
  }

  def batchBilltoDim(records: Array[BillToDim]) : List[PeopleUpsertResult] = {
    var results = new ListBuffer[PeopleUpsertResult]()
    if (records.length > 0) {
      using(ConnectionPool.borrow()) { con =>
        {
          val id = java.util.UUID.randomUUID.toString.replace('-', '_') 
          val st = con.createStatement

          val tbl = st.executeUpdate(s"create temporary table dim_billtos_${id} like dim_billtos ")
          val ps = con.prepareStatement(s"insert into dim_billtos_${id} (billto_key, netsuite_key, name, addr1, addr2, city, state, zip, country, phone) values(?,?,?,?,?, ?,?,?,?,?)")
          for (record <- records) {
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
            ps.addBatch
          }
          ps.executeBatch
          ps.close
          // run a select to get the result, followed by insert and update
          val sqlSelect = s"""
            select a.billto_key as old_key, b.billto_key as new_key 
            from dim_billtos_${id} a, dim_billtos b 
            where a.netsuite_key = b.netsuite_key 
            union all 
            select a.billto_key, a.billto_key 
            from dim_billtos_${id} a 
            where not exists (select 1 from dim_billtos b where b.netsuite_key = a.netsuite_key)
          """
          val sqlInsert = s"""
            insert into dim_billtos (billto_key, netsuite_key, name, addr1, addr2, city, state, zip, country, phone)
            select billto_key, netsuite_key, name, addr1, addr2, city, state, zip, country, phone
            from dim_billtos_${id} a 
            where not exists (select 1 from dim_billtos b where b.netsuite_key = a.netsuite_key) 
          """
          val sqlUpdate = s"""
            update dim_billtos b
            inner join dim_billtos_${id}  a
            on (a.netsuite_key = b.netsuite_key)
            set b.name = a.name, b.addr1 = a.addr1, b.addr2 = a.addr2, b.city = a.city, b.state = a.state, b.zip = a.zip, b.country = a.country, b.phone = a.phone
          """

          val rs: ResultSet = st.executeQuery(sqlSelect)
          while (rs.next) {
            results += new PeopleUpsertResult(
              rs.getString(1),
              rs.getString(2)
            )
          }
          rs.close
          st.executeUpdate(sqlUpdate)
          st.executeUpdate(sqlInsert)
          st.close
        }
      }
    }
    results.toList
  }

  def upsertShiptoDim(
      records: Array[streamer.ShipToDim]
  ): List[PeopleUpsertResult] = {
    var results = new ListBuffer[PeopleUpsertResult]()
    if (records.length > 0) {
      using(ConnectionPool.borrow()) { con =>
        {
          val ps = con.prepareCall("{call sp_upsert_shiptos (?,?,?,?,?, ?,?,?,?,?, ?)}")
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
        }
      }
    }
    results.toList
  }

  def batchShiptoDim(records: Array[streamer.ShipToDim]) : List[PeopleUpsertResult] = {
    var results = new ListBuffer[PeopleUpsertResult]()
    if (records.length > 0) {
      using(ConnectionPool.borrow()) { con =>
        {
          val id = java.util.UUID.randomUUID.toString.replace('-', '_') 
          val st = con.createStatement
          val tbl = st.executeUpdate(s"create temporary table dim_shiptos_${id} like dim_shiptos ")
          val ps = con.prepareStatement(s"insert into dim_shiptos_${id} (shipto_key, netsuite_key, name, addr1, addr2, city, state, zip, country, phone, desttype_key) values(?,?,?,?,?, ?,?,?,?,?, ?)")
          for (record <- records) {
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
            ps.addBatch
          }
          ps.executeBatch
          ps.close
          // run a select to get the result, followed by insert and update
          val sqlSelect = s"""
            select a.shipto_key as old_key, b.shipto_key as new_key 
            from dim_shiptos_${id} a, dim_shiptos b 
            where a.netsuite_key = b.netsuite_key 
            union all 
            select a.shipto_key, a.shipto_key 
            from dim_shiptos_${id} a 
            where not exists (select 1 from dim_shiptos b where b.netsuite_key = a.netsuite_key)
          """
          val sqlInsert = s"""
            insert into dim_shiptos (shipto_key, netsuite_key, name, addr1, addr2, city, state, zip, country, phone, desttype_key)
            select shipto_key, netsuite_key, name, addr1, addr2, city, state, zip, country, phone, desttype_key
            from dim_shiptos_${id} a 
            where not exists (select 1 from dim_shiptos b where b.netsuite_key = a.netsuite_key) 
          """
          val sqlUpdate = s"""
            update dim_shiptos b
            inner join dim_shiptos_${id}  a
            on (a.netsuite_key = b.netsuite_key)
            set b.name = a.name, b.addr1 = a.addr1, b.addr2 = a.addr2, b.city = a.city, b.state = a.state, b.zip = a.zip, b.country = a.country, b.phone = a.phone, b.desttype_key = a.desttype_key
          """

          val rs: ResultSet = st.executeQuery(sqlSelect)
          while (rs.next) {
            results += new PeopleUpsertResult(
              rs.getString(1),
              rs.getString(2)
            )
          }
          rs.close
          st.executeUpdate(sqlUpdate)
          st.executeUpdate(sqlInsert)
          st.close
        }
      }
    }
    results.toList
  }

  def upsertDSRFact(records: Array[streamer.DailySalesFact]): Unit = {
    if (records.length > 0) {
      using(ConnectionPool.borrow()) { con =>
        {
          val ps = con.prepareCall("{call sp_upsert_dsr (?,?,?,?,?,?,?,?,?,?)}")
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
            ps.setDouble(9, record.discount)
            ps.setDouble(10, record.shipping)
            ps.addBatch()
          }
          ps.executeBatch
          ps.close
        }
      }
    }
  }

  def upsertOrdersFact(records: Array[streamer.OrdersFact]): Unit = {
    if (records.length > 0) {
      using(ConnectionPool.borrow()) { con =>
        {
          val ps = con.prepareCall("{call sp_upsert_orders (?,?,?,?,?,?,  ?,?,?,?,?,?,?,  ?,?,?,?,?,?)}")
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
            ps.addBatch()
          }
          ps.executeBatch()
          ps.close
        }
      }
    }
  }

  def batchOrderLinesFact(records: Array[OrderLineFact], result: Boolean) : List[LineUpsertResult] = {
    var results = new ListBuffer[LineUpsertResult]()
    if (records.length > 0) {
      using(ConnectionPool.borrow()) { con =>
        {
          val id = java.util.UUID.randomUUID.toString.replace('-', '_') 
          val st = con.createStatement

          val tbl = st.executeUpdate(s"create temporary table fact_orderlines_${id} like fact_orderlines ")
          val ps = con.prepareStatement(s"""
            insert into fact_orderlines_${id} 
            (date_key,channel_key,source_key,school_key,customer_key,product_key,billto_key,shipto_key,netsuite_order_id,netsuite_order_number,
            ocm_order_id,ocm_order_number,shipment_number,netsuite_line_id,netsuite_line_key,lob_key,desttype_key,is_dropship,total_price,
            total_tax,total_cost,quantity,is_discount,is_shipping,is_service,is_cancelled,total_discount,total_shipping) 
            values(?,?,?,?,?,?,?,?,  ?,?,?,?,?,?,?,?,?,?,  ?,?,?,?,?,?,?,?, ?,?)
            """
          )
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
            ps.setDouble(27, record.total_discount)
            ps.setDouble(28, record.total_shipping)     
            ps.addBatch
          }
          ps.executeBatch
          ps.close
          // run a select to get the result, followed by insert and update
          val sqlSelect = s"""
            select b.netsuite_line_id as line_id, 1 as existing, b.is_cancelled as cancelled, b.total_price as price, 
            b.total_tax as tax, b.total_cost as cost, b.total_discount as discount, b.total_shipping as shipping
            from fact_orderlines_${id} a, fact_orderlines b 
            where a.netsuite_line_id = b.netsuite_line_id 
            union all 
            select a.netsuite_line_id as line_id, 0 as existing, a.is_cancelled as cancelled, a.total_price as price, 
            a.total_tax as tax, a.total_cost as cost, a.total_discount as discount, a.total_shipping as shipping
            from fact_orderlines_${id} a 
            where not exists (select 1 from fact_orderlines b where b.netsuite_line_id = a.netsuite_line_id)
          """
          val sqlInsert = s"""
            insert into fact_orderlines (date_key,channel_key,source_key,school_key,customer_key,product_key,billto_key,shipto_key,netsuite_order_id,netsuite_order_number,
            ocm_order_id,ocm_order_number,shipment_number,netsuite_line_id,netsuite_line_key,lob_key,desttype_key,is_dropship,total_price,
            total_tax,total_cost,quantity,is_discount,is_shipping,is_service,is_cancelled,total_discount,total_shipping) 
            select date_key,channel_key,source_key,school_key,customer_key,product_key,billto_key,shipto_key,netsuite_order_id,netsuite_order_number,
            ocm_order_id,ocm_order_number,shipment_number,netsuite_line_id,netsuite_line_key,lob_key,desttype_key,is_dropship,total_price,
            total_tax,total_cost,quantity,is_discount,is_shipping,is_service,is_cancelled,total_discount,total_shipping
            from fact_orderlines_${id} a 
            where not exists (select 1 from fact_orderlines b where b.netsuite_line_id = a.netsuite_line_id) 
          """
          val sqlUpdate = s"""
            update fact_orderlines b
            inner join fact_orderlines_${id} a
            on (a.netsuite_line_id = b.netsuite_line_id)
            set b.date_key = a.date_key,
            b.channel_key = a.channel_key,
            b.source_key = a.source_key,
            b.school_key = a.school_key,
            b.customer_key = a.customer_key,
            b.product_key = a.product_key,
            b.billto_key = a.billto_key,
            b.shipto_key = a.shipto_key,
            b.netsuite_order_id = a.netsuite_order_id,
            b.netsuite_order_number = a.netsuite_order_number,
            b.ocm_order_id = a.ocm_order_id,
            b.ocm_order_number = a.ocm_order_number,
            b.shipment_number = a.shipment_number,
            b.lob_key = a.lob_key,
            b.desttype_key = a.desttype_key,
            b.is_dropship = a.is_dropship,
            b.total_price = a.total_price,
            b.total_tax = a.total_tax,
            b.total_cost = a.total_cost,
            b.quantity = a.quantity,
            b.is_discount = a.is_discount,
            b.is_shipping = a.is_shipping,
            b.is_service = a.is_service,
            b.is_cancelled = a.is_cancelled,
            b.total_discount = a.total_discount,
            b.total_shipping = a.total_shipping
          """
          if (result) {
            val rs: ResultSet = st.executeQuery(sqlSelect)
            while (rs.next) {
              results += new LineUpsertResult(
                  rs.getString(1),
                  rs.getBoolean(2),
                  rs.getBoolean(3),
                  rs.getDouble(4),
                  rs.getDouble(5),
                  rs.getDouble(6),
                  rs.getDouble(7),
                  rs.getDouble(8)
              )
            }
            rs.close
          }
          st.executeUpdate(sqlUpdate)
          st.executeUpdate(sqlInsert)
          st.close
        }
      }
    }
    results.toList
  }  

  def upsertOrderLinesFact(
      records: Array[streamer.OrderLineFact]
  ): List[LineUpsertResult] = {
    var results = new ListBuffer[LineUpsertResult]()
    if (records.length > 0) {
      using(ConnectionPool.borrow()) { con =>
        {
          val ps =
            con.prepareCall("{call sp_upsert_orderlines (?,?,?,?,?,?,?,?,  ?,?,?,?,?,?,?,?,?,?,  ?,?,?,?,?,?,?,?, ?,?)}")
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
            ps.setDouble(27, record.total_discount)
            ps.setDouble(28, record.total_shipping)            

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
                rs.getDouble(6),
                rs.getDouble(7),
                rs.getDouble(8)
              )
              results += result
            }
          }
          ps.close
        }
      }
    }
    results.toList
  }

}
