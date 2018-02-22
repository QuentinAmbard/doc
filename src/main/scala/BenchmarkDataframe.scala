import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, _}
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator

object typedExtended {
  def max[IN](f: IN => Long): TypedColumn[IN, Long] = new TypedMaxLong(f).toColumn
}

class TypedMaxLong[IN](val f: IN => Long) extends Aggregator[IN, Long, Long] {
  override def zero: Long = 0L

  override def reduce(b: Long, a: IN): Long = Math.max(b, f(a))

  override def merge(b1: Long, b2: Long): Long = Math.max(b1, b2)

  override def finish(reduction: Long): Long = reduction

  override def bufferEncoder: Encoder[Long] = ExpressionEncoder[Long]()

  override def outputEncoder: Encoder[Long] = ExpressionEncoder[Long]()

  // Java api support
  def this(f: MapFunction[IN, java.lang.Long]) = this(x => f.call(x).asInstanceOf[Long])

  def toColumnJava: TypedColumn[IN, java.lang.Long] = {
    toColumn.asInstanceOf[TypedColumn[IN, java.lang.Long]]
  }
}

case class Purchase1(user_id: Long,price: Int,item: String)

case class User1(user_id: Long, firstname: String, lastname: String)


object BenchmarkDataframe {

  val spark = BenchmarkHelper.spark

  import spark.implicits._

  var timeit = 1

  def main(args: Array[String]) = {
    if (args.length > 0) timeit = args(0).toInt
    val purchases = spark.sparkContext.cassandraTable(DataLoader.Model.ks, DataLoader.Model.purchaseTable).select("user_id", "price", "item").cache
    purchases.count()
    val rdd = getRDD(purchases)
    purchases.unpersist()
    val purchasesDF = spark.read.cassandraFormat(DataLoader.Model.purchaseTable, DataLoader.Model.ks).load().select("user_id", "price", "item").cache
    purchasesDF.rdd.count()
    val measure = Measure(Seq(s"max purchase per item", s"sum (max purchase per user)", "to lowercase", "UDF concat"), Seq(rdd, getDataframe(purchasesDF), getDataset(purchasesDF)))
    println(Json.mapper.writeValueAsString(measure))
    spark.stop()
  }


  private def getRDD(purchases: CassandraTableScanRDD[CassandraRow]) = {
    val maxPricePerUserRDD = TimeitUtils.timeIt(timeit) {
      val maxs = purchases.map(r => (r.getString("item").substring(0,2), (r.getLong("user_id"),r.getLong("price"),r.getString("item"))))
        .reduceByKey((max, r) => if (r._2 > max._2) r else max)
      val maxCount = maxs.count()
      println(s"maxPurchasePerFirstLetter $maxCount")
    }

    val maxPriceRDD = TimeitUtils.timeIt(timeit) {
      val maxs = purchases.map(r => (r.getString("item").substring(0,2), r.getLong("price")))
        .reduceByKey((max, r) => Math.max(max,r))
      val max = maxs.aggregate(0L)((sum, max) => sum + max._2, (max1, max) => max1 + max)
      println(s"max=$max")
    }
    val lowerRDD = TimeitUtils.timeIt(timeit) {
      val purchaseCount = purchases
        .map(r => (r.getLong("user_id"), r.getString("item").toLowerCase))
        .count()
      println(s"lowercase $purchaseCount")
    }
    val concatRDD = TimeitUtils.timeIt(timeit) {
     val purchaseCount = purchases
        .map(r => s"${r.getLong("user_id")}-${r.getString("item")}-${r.getLong("price")}")
        .count()
      println(s"concat $purchaseCount")
    }

    val distinctRDD = TimeitUtils.timeIt(timeit) {
      purchases.map(r => r.getString("item").substring(0, 2)).distinct().count()
    }

    Dataset("RDD", Seq(maxPricePerUserRDD, maxPriceRDD, lowerRDD,concatRDD,distinctRDD))
  }

  private def getDataframe(purchases: DataFrame) = {
    import org.apache.spark.sql.functions._
    val maxPricePerUserDataframe = TimeitUtils.timeIt(timeit) {
      val maxCount = purchases
        .groupBy(substring($"item",0,2)).agg(max("price")).rdd.count()
      println(s"maxPurchasePerFirstLetter $maxCount")
    }
    import org.apache.spark.sql.functions._
    import spark.sqlContext.implicits._
    val maxPriceDataframe = TimeitUtils.timeIt(timeit) {
      purchases.groupBy(substring($"item",0,2)).agg(max("price") as "max").select(sum("max")).show()
    }

    import org.apache.spark.sql.functions._
    val lowerDataframe = TimeitUtils.timeIt(timeit) {
      val userCount = purchases
        .withColumn("item", lower($"item"))
        .rdd.count()
      println(s"lowercase $userCount")
    }

    import org.apache.spark.sql.functions._
    val concat = udf[String, Int, Int, String]((user_id, price, item) => s"$user_id-$price-$item")
    val concatDataframe = TimeitUtils.timeIt(timeit) {
      val userCount = purchases
        .withColumn("result", concat($"user_id", $"price", $"item"))
        .rdd.count()
      println(s"concat $userCount")
    }

    val distinctDataframe = TimeitUtils.timeIt(timeit) {
      val distinct=purchases.drop("user_id", "price").withColumn("item",substring($"item",0,2)).distinct().rdd.count()
      println(s"distinct=$distinct")
    }

    Dataset("dataframe", Seq(maxPricePerUserDataframe, maxPriceDataframe, lowerDataframe, concatDataframe, distinctDataframe))
  }

  private def getDataset(purchases: DataFrame) = {

    val maxPriceDataset = TimeitUtils.timeIt(timeit) {
      val ds: sql.Dataset[Purchase1] = purchases.as[Purchase1]
      val maxs: (String, Long) = ds.map(p => (p.item.substring(0,2), p.price)).groupByKey(_._1).agg(typedExtended.max(_._2)).reduce((a, b) => ("", a._2 + b._2))
      println(s"maxs=$maxs")
    }

    val lowerDataset = TimeitUtils.timeIt(timeit) {
      import spark.implicits._
      val userCount = purchases.as[Purchase1]
        .map(p => (p.user_id, p.item.toLowerCase, p.price))
        .rdd.count()
      println(s"lowercase $userCount")
    }
    val concatDataset = TimeitUtils.timeIt(timeit) {
      val userConcat = purchases.as[Purchase1]
        .map(p => s"${p.user_id}-${p.item}-${p.price}")
        .rdd.count()
      println(s"concat $userConcat")
    }

    val distinctDataset = TimeitUtils.timeIt(timeit) {
      val distinct = purchases.as[Purchase1]
        .map(p => p.item.substring(0,2)).distinct()
        .rdd.count()
      println(s"distinct $distinct")
    }

    Dataset("Dataset", Seq(0, maxPriceDataset, lowerDataset, concatDataset, distinctDataset))
  }


}

