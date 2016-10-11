package poc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.Logging
import com.datastax.spark.connector._


/**
  * Hello world example
  * Read from cassandra to make sure things are working.
  *
  */


//spark.master local[4]
//spark.executor.memory 1g
//spark.cassandra.connection.host 127.0.0.1
//#spark.cassandra.auth.username
//#spark.cassandra.auth.password
//spark.serializer org.apache.spark.serializer.KryoSerializer
//spark.eventLog.enabled true
//spark.eventLog.dir /var/tmp/eventLog

//val conf = new SparkConf(true)
//.set("spark.cassandra.connection.host", "192.168.123.10")
//.set("spark.cassandra.auth.username", "cassandra")
//.set("spark.cassandra.auth.password", "cassandra")
//
//val sc = new SparkContext("spark://192.168.123.10:7077", "test", conf)


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
