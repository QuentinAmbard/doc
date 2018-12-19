import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import org.apache.spark.rdd.RDD


object BenchmarkTest {

  val spark = BenchmarkHelper.spark

  var timeit = 1

  def main(args: Array[String]) = {
    if (args.length > 0) timeit = args(0).toInt
    println("RDD join span by key")
    val userJoinPurchaseSamePartitionerSpan = TimeitUtils.timeIt(timeit) {
//      val purchases: RDD[(Long, Iterable[CassandraRow])] = spark.sparkContext.cassandraTable(DataLoader.Model.ks, DataLoader.Model.purchaseTable).spanBy(r => r.getLong("user_id"))
//      purchases.take(10).foreach(println)
//      val test: RDD[(Long, CassandraRow)] = spark.sparkContext.cassandraTable(DataLoader.Model.ks, DataLoader.Model.purchaseTable).keyBy(r => r.getLong("user_id"))
//      println(test.toDebugString)
//      println(test.partitioner)
//      test.take(100).foreach(println)
//      println("--------")
      //val test2: CassandraTableScanRDD[(Tuple1[Long], CassandraRow)] = spark.sparkContext.cassandraTable(DataLoader.Model.ks, DataLoader.Model.purchaseTable).keyBy("user_id")

      //spark.sparkContext.cassandraTable[(Int, String, String)]("test", "tab").select("key", "col1", "col2", "col2").keyBy("key", "col1").take(100).foreach(println)
      spark.sparkContext.cassandraTable[(Long, Long, String, Int)](DataLoader.Model.ks, DataLoader.Model.purchaseTable).keyBy[Tuple1[Long]]("user_id").take(10).foreach(println)
      println(spark.sparkContext.cassandraTable[(Long, Long, String, Int)](DataLoader.Model.ks, DataLoader.Model.purchaseTable).keyBy[Tuple1[Long]]("user_id").partitioner)
//      println(test2.toDebugString)
//      println(test2.partitioner)
      //val test3: CassandraTableScanRDD[(Long, CassandraRow)] = spark.sparkContext.cassandraTable(DataLoader.Model.ks, DataLoader.Model.purchaseTable).keyAndApplyPartitionerFrom("user_id")
      //val test = spark.sparkContext.cassandraTable(DataLoader.Model.ks, DataLoader.Model.purchaseTable).keyBy(r => (r.getLong("user_id"), r.getLong("purchase_id")))
      //test2.take(100).foreach(println)
    }
  }
}

