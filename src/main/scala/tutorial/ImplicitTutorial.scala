package tutorial

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{max, substring}
import org.slf4j.{Logger, LoggerFactory}

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

object ImplicitTutorial extends App {

  implicit class RDDPrintFunction[ROW: ClassTag](rdd: RDD[ROW]) {
    def printAll() = rdd.foreach(println)
  }

  //Implicit examples:
  val spark = SparkSession.builder.master("local[2]").getOrCreate()

  // new RDDPrintFunction(purchases).printAll()

  //Spark cassandra implicits:
  import com.datastax.spark.connector._
  import org.apache.spark.sql.cassandra._
  //Spark implicits, note that it's from the spark object
  import spark.implicits._
  //CassandraConnector is implicitly passed
  {
    implicit val connector = CassandraConnector(spark.sparkContext.getConf)
    val purchases = spark.sparkContext.cassandraTable("test_spark", "purchase").select("usr_id", "price", "item")
    //==> spark.sparkContext.cassandraTable("test_spark", "purchase")(connector, ...Other implicits...).select("user_id", "price", "item")
    purchases.printAll() //==>
  }
  val purchasesDF = spark.read.cassandraFormat("purchase", "test_spark").load().select("user_id", "price", "item").groupBy(substring($"item",0,2)).agg(max("price")).rdd.count()



}

