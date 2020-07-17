package streamer

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext

object SqlContextSingleton {
  @transient private var instance: SQLContext = null
  // Instantiate SQLContext on demand
  def getInstance(sc: SparkContext): SQLContext =
    synchronized {
        if (instance == null) {
            instance = new SQLContext(sc)
        }
        instance
    }
}
