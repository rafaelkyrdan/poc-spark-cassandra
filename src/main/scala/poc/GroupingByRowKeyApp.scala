package poc

import com.datastax.spark.connector._
import org.apache.spark._

/**
  * Grouping  by Cassandra's rowKey
  */

object GroupingByRowKeyApp {

  def main(args: Array[String]): Unit = {

    var conf = None: Option[SparkConf]
    var sc = None: Option[SparkContext]

    try {

      conf = Some(new SparkConf()
        .setMaster("local[2]")
        .setAppName("Grouping by rowKey")
        .set("spark.app.id", "grouping")
        .set("spark.cassandra.connection.host", "127.0.0.1")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.eventLog.enabled", "true")
        .set("spark.eventLog.dir", "./output/log")
      )

      sc = Some(new SparkContext(conf.get))

      val visits = sc.get.cassandraTable[(String)]("test", "user_visits").select("user")
      val visitsPerUser = visits.map { user =>
        (user, 1)
      }.reduceByKey(_ + _)

      val maxVisits = visitsPerUser.values.max
      println(maxVisits)

    } finally {
      // Always stop Spark Context explicitly
      if (sc.isDefined) sc.get.stop
    }
  }
}