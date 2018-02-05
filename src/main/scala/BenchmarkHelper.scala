import java.util.Random

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.thedeanda.lorem.Lorem
import org.apache.spark.sql.SparkSession


case class User(user_id : Long, shop_id: Long, firstname: String, lastname: String, zipcode: String, title: String)
case class Purchase(user_id : Long, purchase_id: Long, item: String, price: Int)
case class Shop(shop_id : Long, name: String, description: String)

object BenchmarkHelper {

    def ks = "test_spark"
    def spark = SparkSession.builder
      .master("local[2]")
      .appName("testStructured")
      .config("spark.executor.memory", "1G")
      .config("spark.cassandra.connection.factory", "CustomConnectionFactory")
      .getOrCreate()
}

