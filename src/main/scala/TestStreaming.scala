import java.util.Date

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.SqlRowWriter
import com.esotericsoftware.minlog.Log
import com.esotericsoftware.minlog.Log.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cassandra._
import org.slf4j.LoggerFactory

// For DSE it is not necessary to set connection parameters for spark.master (since it will be done
// automatically)

case class WordCount(word: String, count: Long, time: Long)
case class WordCountAggregate(word: String, count: Long)

object SimpleSparkStreaming {

  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Usage: SimpleSparkStreaming <hostname> <port> <seconds> <persist> <aggregate>")
      System.exit(1)
    }

    val persist = args(3).toBoolean
    val aggregate = args(4).toBoolean

    val conf = new SparkConf().setAppName("SimpleSparkStreaming")
    conf.set("spark.locality.wait", "0");
    conf.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
    conf.registerKryoClasses(Array(classOf[WordCount],classOf[WordCountAggregate]))


    val sc = SparkContext.getOrCreate(conf)


    var seconds = 1
    if (args.length > 2) {
      seconds = args(2).toInt
    }

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext(sc, Seconds(seconds))
    if (aggregate) {
      ssc.checkpoint("/tmp/checkpoint")
    }

    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)

    val words = lines.flatMap(_.split(",")).map(x => (x.trim(), 1))
    val wordCounts = words.reduceByKey(_ + _)

    wordCounts.foreachRDD { (rdd: RDD[(String, Int)], time: org.apache.spark.streaming.Time) =>
      //Log.setLogger(new MyLogger())
      //Log.TRACE()
      val epochTime: Long = time.milliseconds


      val wordCountsRDD = rdd.map((r: (String, Int)) => WordCount(r._1, r._2, epochTime))
      wordCountsRDD.take(10).foreach(println)
      if (persist) {
        wordCountsRDD.saveToCassandra("wordcount", "wordcount")
      }
    }


    if (aggregate) {
      def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
        val newCount = runningCount.getOrElse(0) + newValues.sum
        Some(newCount)
      }
      var stateCount = words.updateStateByKey[Int](updateFunction _).map(x => Row(x._1, x._2))

      stateCount.foreachRDD { (rdd: RDD[Row], time: org.apache.spark.streaming.Time) =>

        val wordCountsRDD = rdd.map((r: (Row)) => WordCountAggregate(r.getAs[String](0), r.getAs[Int](1)))
        print(wordCountsRDD.take(10))
        if(persist){
          wordCountsRDD.saveToCassandra("wordcount", "rollups")
        }
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
// scalastyle:on println


/*
/** Lazily instantiated singleton instance of SparkSession */
object SparkSessionSingleton {
  @transient  private var instance: SparkSession = _
  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }
}
class MyLogger() extends Logger() {
  override def log(level:Int, category:String, message:String, ex:Throwable) {
    var builder = new StringBuilder(256)
    var factory = LoggerFactory.getLogger("SimpleStreaming")
    factory.error(message)
  }
}
*/