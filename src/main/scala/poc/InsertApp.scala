package poc

import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Random


/**
  * Insert data into Cassandra
  */

object InsertApp {

  def main(args: Array[String]): Unit = {

    var conf = None: Option[SparkConf]
    var sc = None: Option[SparkContext]

    try {

      conf = Some(new SparkConf()
        .setMaster("local[2]")
        .setAppName("Insert Data into Cassandra")
        .set("spark.app.id", "insert")
        .set("spark.cassandra.connection.host", "127.0.0.1")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.eventLog.enabled", "true")
        .set("spark.eventLog.dir", "./output/log")
      )

      sc = Some(new SparkContext(conf.get))

      val numStores = 128

      randomStores(sc.get, stores = numStores, cities = 32)
        .saveToCassandra("test", "stores", SomeColumns("city", "store", "manager"))

      randomVisits(sc.get, users = 16384, visitsPerUser = 128, stores = numStores)
        .saveToCassandra("test", "user_visits", SomeColumns("user", "utc_millis", "store"))

    } finally {
      // Always stop Spark Context explicitly
      if (sc.isDefined) sc.get.stop
    }
  }

  def randomStores(sc: SparkContext, stores: Int, cities: Int): RDD[(String, String, String)] = {
    sc.parallelize(0 until stores).map { s =>
      val city = s"city_${Random.nextInt(cities)}"
      val store = s"store_${s}"
      val manager = s"manager_${Math.abs(Random.nextInt)}"

      (city, store, manager)
    }
  }

  def randomVisits(sc: SparkContext, users: Int, stores: Int, visitsPerUser: Int): RDD[(String, Long, String)] = {
    sc.parallelize(0 until users).flatMap { u =>
      val user = s"user_${u}"
      (1 to visitsPerUser).map { v =>
        val utcMillis = System.currentTimeMillis - Random.nextInt
        val store = s"store_${Random.nextInt(stores)}"
        (user, utcMillis, store)
      }
    }
  }
}
