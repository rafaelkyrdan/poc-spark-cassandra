package poc

import com.datastax.spark.connector._
import org.apache.spark._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * Aggregating using mapPartitions
  */

object GroupingOnPartitions {

  def main(args: Array[String]): Unit = {

    var conf = None: Option[SparkConf]
    var sc = None: Option[SparkContext]

    try {

      conf = Some(new SparkConf()
        .setMaster("local[2]")
        .setAppName("Grouping on Partitions")
        .set("spark.app.id", "grouping_on_partitions")
        .set("spark.cassandra.connection.host", "127.0.0.1")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.eventLog.enabled", "true")
        .set("spark.eventLog.dir", "./output/log")
      )

      sc = Some(new SparkContext(conf.get))

      val visits = sc.get.cassandraTable[(String)]("test", "user_visits").select("user")
      val visitsPerUser = visits.map { user =>
        (user, 1)
      }.mapPartitions { userIterator =>
        new GroupByKeyIterator(userIterator)
      }.mapValues(_.size)

      val maxVisits = visitsPerUser.values.max
      println(maxVisits)

    } finally {
      // Always stop Spark Context explicitly
      if (sc.isDefined) sc.get.stop
    }
  }
}

/**
  * Groups values by key.
  *
  */

class GroupByKeyIterator[K, V: ClassTag](it: Iterator[Product2[K, V]]) extends Iterator[(K, Array[V])] {

  var key: Option[K] = None
  val values = new ArrayBuffer[V]()

  override def hasNext: Boolean = it.hasNext || key.isDefined

  override def next: (K, Array[V]) = {

    while (it.hasNext) {
      val (nextKey, nextValue) = it.next
      if (key.map(_ == nextKey).getOrElse(false)) {
        values.append(nextValue)
      } else {
        val result = key.map(k => (k, values.toArray))
        key = Some(nextKey)
        values.clear
        values.append(nextValue)
        if (result.isDefined) return result.get
      }
    }

    val result = key.map(k => (k, values.toArray))
    key = None
    result.getOrElse(throw new java.util.NoSuchElementException("next on empty iterator"))
  }
}

