import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


case class User(user_id: Long, shop_id: Long, firstname: String, lastname: String, zipcode: String, title: String)


case class Shop(shop_id: Long, shop_name: String, description: String)

case class Purchase(user_id: Long, purchase_id: Long, item: String, price: Int)

case class Dataset(label: String, data: Seq[Long])

case class Measure(labels: Seq[String], datasets: Seq[Dataset])

object BenchmarkHelper {

  def spark = SparkSession.builder
    .master("local[2]")
    //.config("spark.sql.shuffle.partitions", "400")
    .appName("test_spark")
      .config(new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[TestJAva])))

    //.config("spark.cassandra.connection.factory", "CustomConnectionFactory")
    .getOrCreate()
}

object Json {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
}
