import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import org.apache.spark.sql
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._


object BenchmarkDataframePartition {

  val spark = BenchmarkHelper.spark

  import spark.implicits._

  var timeit = 10

  def main(args: Array[String]) = {
    if (args.length > 0) timeit = args(0).toInt
    spark.read.format("org.apache.spark.sql.cassandra").options(Map(
      "table" -> DataLoader.Model.purchaseTable,
      "keyspace" -> DataLoader.Model.ks,
      "spark.sql.shuffle.partitions" -> "400",
      "spark.cassandra.input.split.size_in_mb" -> "64"))
      .load().repartition()
      .select("user_id", "price", "item")
      .groupBy(substring($"item", 0, 2)).agg(max("price") as "max").select(sum("max")).show()

    spark.read.format("org.apache.spark.sql.cassandra").options(Map(
      "table" -> DataLoader.Model.userTable,
      "keyspace" -> DataLoader.Model.ks,
      "spark.sql.shuffle.partitions" -> "300",
      "spark.cassandra.input.split.size_in_mb" -> "64"))
      .load()
      .select("user_id", "firstname")
      .groupBy($"firstname").agg(avg("user_id") as "avg").select(sum("avg")).show()
    spark.stop()
  }
}

