package poc

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._

/**
  * Join 2 Cassandra's tables
  */

object Join2TablesApp {

  def main(args: Array[String]): Unit = {

    var conf = None: Option[SparkConf]
    var sc = None: Option[SparkContext]

    try {

      conf = Some(new SparkConf()
        .setMaster("local[2]")
        .setAppName("Join 2 Tables")
        .set("spark.app.id", "hello world")
        .set("spark.cassandra.connection.host", "127.0.0.1")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.eventLog.enabled", "true")
        .set("spark.eventLog.dir", "./output/log")
      )

      sc = Some(new SparkContext(conf.get))

      val visits = sc.get
        .cassandraTable[(String, String)]("test", "user_visits")
        .select("store", "user")

      val stores = sc.get
        .cassandraTable[(String, String)]("test", "stores")
        .select("store", "city")

      val visitsPerCity = visits.join(stores).map {
        case (store, (user, city)) => (city, 1)
      }.reduceByKey(_ + _)

      val result = visitsPerCity.collect
      result.foreach(println)

    } finally {
      // Always stop Spark Context explicitly
      if (sc.isDefined) sc.get.stop
    }
  }
}
