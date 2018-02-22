import com.datastax.spark.connector._
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql._
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

case class Purchase1(item: String, price: Int)

case class User1(user_id: Long, firstname: String, lastname: String)


object BenchmarkDataframe {

  val spark = BenchmarkHelper.spark

  import spark.implicits._

  var timeit = 1

  def main(args: Array[String]) = {
    if (args.length > 0) timeit = args(0).toInt
    //Warmup
    spark.sparkContext.cassandraTable(DataLoader.Model.ks, DataLoader.Model.purchaseTable).count()
    val measure = Measure(Seq(s"RDD", s"Dataframe", "Dataset"), Seq(sumOfMaxPurchasePerUser(), maxPurchasePerItem(), lowercase(), udfConcat()))
    println(Json.mapper.writeValueAsString(measure))
    spark.stop()
  }


  private def maxPurchasePerItem() = {
    val maxPriceRDD = TimeitUtils.timeIt(timeit) {
      val purchases: RDD[(String, (Int, Int, String))] = spark.sparkContext.cassandraTable(DataLoader.Model.ks, DataLoader.Model.purchaseTable)
          .map(r => (r.getString("item").substring(0,2), (r.getInt("user_id"),r.getInt("price"),r.getString("item"))))
      val maxs = purchases.reduceByKey((max, r) => if (r._2 > max._2) r else max)
      val maxCount = maxs.collect().length
      println(s"maxPurchasePerFirstLetter $maxCount")
    }
    import org.apache.spark.sql.functions._
    val maxPriceDataframe = TimeitUtils.timeIt(timeit) {
      val maxCount = spark.read.cassandraFormat(DataLoader.Model.purchaseTable, DataLoader.Model.ks).load().select("user_id", "purchase_id", "item", "price")
          .withColumn("item", substring($"item",0,2)).groupBy("item").agg(max("price")).collect().length
      println(s"maxPurchasePerFirstLetter $maxCount")
    }

    Dataset("max purchase per item", Seq(maxPriceRDD, maxPriceDataframe,0))
  }

  private def sumOfMaxPurchasePerUser() = {
    val maxPriceRDD = TimeitUtils.timeIt(timeit) {
      val purchases: RDD[(String, Int)] = spark.sparkContext.cassandraTable(DataLoader.Model.ks, DataLoader.Model.purchaseTable).select("item", "price")
        .map(r => (r.getString("item").substring(0,2), r.getInt("price")))
      val maxs = purchases.reduceByKey((max, r) => Math.max(max,r))
      val max = maxs.aggregate(0)((sum, max) => sum + max._2, (max1, max) => max1 + max)
      println(s"max=$max")
    }
    import org.apache.spark.sql.functions._
    import spark.sqlContext.implicits._
    val maxPriceDataframe = TimeitUtils.timeIt(timeit) {
      val maxCount = spark.read.cassandraFormat(DataLoader.Model.purchaseTable, DataLoader.Model.ks).load().select("item", "price")
        .withColumn("item", substring($"item",0,2)).groupBy("item").agg(max("price") as "max").select(sum("max")).show()
    }

    val maxPriceDataset = TimeitUtils.timeIt(timeit) {
      val ds: sql.Dataset[Purchase1] = spark.read.cassandraFormat(DataLoader.Model.purchaseTable, DataLoader.Model.ks).load().select("item", "price").as[Purchase1]
      val maxs: (String, Long) = ds.map(p => (p.item.substring(0,2), p.price)).groupByKey(_._1).agg(typedExtended.max(_._2)).reduce((a, b) => ("", a._2 + b._2))
      println(s"maxs=$maxs")
    }
    Dataset("sum (max purchase per user)", Seq(maxPriceRDD, maxPriceDataframe, maxPriceDataset))
  }

  private def lowercase() = {

    val lowerRDD = TimeitUtils.timeIt(timeit) {
      val userCount = spark.sparkContext.cassandraTable(DataLoader.Model.ks, DataLoader.Model.userTable).select("user_id", "firstname", "lastname")
        .map(r => (r.getInt("user_id"), r.getString("firstname").toLowerCase, r.getString("lastname").toLowerCase))
        .collect().length
      println(s"lowercase $userCount")
    }
    import org.apache.spark.sql.functions._
    val lowerDataframe = TimeitUtils.timeIt(timeit) {
      val userCount = spark.read.cassandraFormat(DataLoader.Model.userTable, DataLoader.Model.ks).load().select("user_id", "firstname", "lastname")
        .withColumn("lastname", lower($"lastname")).withColumn("firstname", lower($"firstname"))
        .collect().length
      println(s"lowercase $userCount")
    }

    val lowerDataset = TimeitUtils.timeIt(timeit) {
      import spark.implicits._
      val userCount = spark.read.cassandraFormat(DataLoader.Model.userTable, DataLoader.Model.ks).load().select("user_id", "firstname", "lastname").as[User1]
        .map(p => (p.user_id, p.lastname.toLowerCase, p.firstname.toLowerCase))
        .collect().length
      println(s"lowercase $userCount")
    }
    Dataset("to lowercase", Seq(lowerRDD, lowerDataframe, lowerDataset))
  }


  private def udfConcat() = {

    val concatRDD = TimeitUtils.timeIt(timeit) {
      val userCount = spark.sparkContext.cassandraTable(DataLoader.Model.ks, DataLoader.Model.userTable).select("user_id", "firstname", "lastname")
        .map(r => s"${r.getInt("user_id")}-${r.getString("firstname")}-${r.getString("lastname")}")
        .collect().length
      println(s"concat $userCount")
    }
    import org.apache.spark.sql.functions._
    val concat = udf[String, Int, String, String]((user_id, firstname, lastname) => s"$user_id-$firstname-$lastname")
    val concatDataframe = TimeitUtils.timeIt(timeit) {
      val userCount = spark.read.cassandraFormat(DataLoader.Model.userTable, DataLoader.Model.ks).load().select("user_id", "firstname", "lastname")
        .withColumn("result", concat($"lastname", $"firstname", $"firstname"))
        .collect().length
      println(s"concat $userCount")
    }

    val concatDataset = TimeitUtils.timeIt(timeit) {
      val userConcat = spark.read.cassandraFormat(DataLoader.Model.userTable, DataLoader.Model.ks).load().select("user_id", "firstname", "lastname").as[User1]
        .map(p => s"${p.user_id}-${p.firstname}-${p.lastname}")
        .collect().length
      println(s"concat $userConcat")
    }
    Dataset("UDF concat", Seq(concatRDD, concatDataframe, concatDataset))
  }

}

