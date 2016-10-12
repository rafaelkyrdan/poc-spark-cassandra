package poc

import com.datastax.spark.connector._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Broadcast set to all partitions
  */

object BroadcastApp {

  def main(args: Array[String]): Unit = {

    var conf = None: Option[SparkConf]
    var sc = None: Option[SparkContext]

    try {

      conf = Some(new SparkConf()
        .setMaster("local[2]")
        .setAppName("Broadcast example")
        .set("spark.app.id", "broadcast")
        .set("spark.cassandra.connection.host", "127.0.0.1")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.eventLog.enabled", "true")
        .set("spark.eventLog.dir", "./output/log")
      )

      sc = Some(new SparkContext(conf.get))

      val storeToCity = sc.get
        .cassandraTable[(String, String)]("test", "stores")
        .select("store", "city")
        .collect
        .toMap

      val cityOf = sc.get.broadcast(storeToCity)
      val visits = sc.get.cassandraTable[(String, String)]("test", "user_visits")
        .select("store", "user")

      val visitsPerCity = visits.map {
        case (store, user) => (cityOf.value.apply(store), 1)
      }.reduceByKey(_ + _)

      val result = visitsPerCity.collect
      result.foreach(println)

    } finally {
      // Always stop Spark Context explicitly
      if (sc.isDefined) sc.get.stop
    }
  }
}
