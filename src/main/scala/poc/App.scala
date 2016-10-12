package poc

import com.datastax.spark.connector._
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Hello world example
  * Read from cassandra to make sure things are working.
  *
  */

object App {

  def main(args: Array[String]): Unit = {

    var conf = None: Option[SparkConf]
    var sc = None: Option[SparkContext]

    try {

      conf = Some(new SparkConf()
        .setMaster("local[2]")
        .setAppName("Hello World Cassandra")
        .set("spark.app.id", "hello world")
        .set("spark.cassandra.connection.host", "127.0.0.1")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.eventLog.enabled", "true")
        .set("spark.eventLog.dir", "./output/log")
      )

      sc = Some(new SparkContext(conf.get))


      val hello = sc.get.cassandraTable[(String, String)]("test", "hello")
      val first = hello.first
      println(first)


    } finally {
      // Always stop Spark Context explicitly
      if (sc.isDefined) sc.get.stop
    }
  }
}
