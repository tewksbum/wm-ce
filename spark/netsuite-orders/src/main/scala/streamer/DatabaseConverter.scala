package streamer

import java.sql.{Connection,DriverManager}
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext._

object DatabaseConverter {



  val jdbcUrl = "jdbc:mariadb://10.128.0.32:3306/segment?user=spark&password=sMehnXVuJ0LKQcndEtvv"

  def getConnection() : java.sql.Connection = {
    var connection = DriverManager.getConnection(jdbcUrl)
    connection
  }
  

  def saveRDDToDB(orders: Array[NetsuiteOrder], windowLength: Int): Unit = {
    if (orders.length > 0) {
      val con : java.sql.Connection = getConnection ()
      val psOrderFact = con.prepareStatement("insert into test_order (order_number, amount) values (?,?)")
      for (order <- orders) {
        println("saving order")
        psOrderFact.clearParameters()
        psOrderFact.setString(1, order.orderNumber)
        psOrderFact.setDouble(2, order.totals.total)
        psOrderFact.executeUpdate() 
      }
      psOrderFact.close
    }
    println(s"Window ending after ${windowLength} seconds with ${orders.length} orders\n")
  }

  def saveRawToDB(orders: Array[String], windowLength: Int): Unit = {
    println(s"processing ${orders.length} orders")
    if (orders.length > 0) {
      val con : java.sql.Connection = getConnection ()
      val psOrderFact = con.prepareStatement("insert into test_message (message) values (?)")
      orders.foreach( (order: String) => {
        psOrderFact.clearParameters()
        psOrderFact.setString(1, order)
        psOrderFact.executeUpdate() 
        println("committed a save")
        con.commit()
      })
      psOrderFact.close
    }
    println(s"Window ending after ${windowLength} seconds with ${orders.length} orders\n")

  }
}
