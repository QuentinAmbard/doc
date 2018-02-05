import java.util.Random

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.thedeanda.lorem.Lorem
import org.apache.spark.sql.SparkSession


object BenchmarkJoins extends App {

    val spark = BenchmarkHelper.spark
    import spark.implicits._

  def testJoinCardinality(): Unit = {
    val userJoinPurchase = TimeitUtils.timeIt(3){
      spark.sparkContext.cassandraTable[User](BenchmarkHelper.ks, "user").joinWithCassandraTable[Purchase](BenchmarkHelper.ks, "purchase").count()
    }
    val purchaseJoinUser = TimeitUtils.timeIt(3){
      spark.sparkContext.cassandraTable[Purchase](BenchmarkHelper.ks, "purchase").joinWithCassandraTable[User](BenchmarkHelper.ks, "user").count()
    }

    println(userJoinPurchase)
    println(purchaseJoinUser)

  }

  testJoinCardinality()

}

