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

case class Purchase1(user_id: Long, price: Int)

case class User1(user_id: Long, firstname: String, lastname: String)


object BenchmarkDataframe {

  val spark = BenchmarkHelper.spark

  import spark.implicits._

  var timeit = 1

  def main(args: Array[String]) = {
    if (args.length > 0) timeit = args(0).toInt
    val measure = Measure(Seq(s"group by max", s"sum(group by max)", "lowercase"), Seq(sumOfMaxPurchasePerUser(), sumOfMaxPurchasePerUser(), lowercase(), udfConcat()))
    println(Json.mapper.writeValueAsString(measure))
    spark.stop()
  }


  private def maxPurchasePerUser() = {
    val maxPriceRDD = TimeitUtils.timeIt(timeit) {
      val purchases: RDD[(String, CassandraRow)] = spark.sparkContext.cassandraTable(DataLoader.Model.ks, DataLoader.Model.purchaseTable).keyBy(u => u.getString("user_id"))
      val maxs = purchases.aggregateByKey(Array[Any](0, null, null, null))((max, r) => {
        if (r.getInt("price") > max(0).asInstanceOf[Int]) {
          max(0) = r.getInt("price")
          max(1) = r.getString("purchase_id")
          max(2) = r.getString("user_id")
          max(3) = r.getString("item")
        }
        max
      }, (max1, max2) => if (max1(0).asInstanceOf[Int] > max2(0).asInstanceOf[Int]) max1 else max2)
      val maxCount = maxs.collect().length
      println(s"lowercase $maxCount")
    }
    import org.apache.spark.sql.functions._
    val maxPriceDataframe = TimeitUtils.timeIt(timeit) {
      val maxCount = spark.read.cassandraFormat(DataLoader.Model.purchaseTable, DataLoader.Model.ks).load().select("user_id", "purchase_id", "item", "price")
        .groupBy("user_id").agg(max("price")).collect().length
      println(s"lowercase $maxCount")
    }

    Dataset("max purchase per user", Seq(maxPriceRDD, maxPriceDataframe))
  }

  private def sumOfMaxPurchasePerUser() = {
    val maxPriceRDD = TimeitUtils.timeIt(timeit) {
      val purchases: RDD[(String, CassandraRow)] = spark.sparkContext.cassandraTable(DataLoader.Model.ks, DataLoader.Model.purchaseTable).select("user_id", "price").keyBy(u => u.getString("user_id"))
      val maxs = purchases.aggregateByKey(0)((max, r) => {
        Math.max(r.getInt("price"), max)
      }, (max1, max2) => Math.max(max1, max2))
      val max = maxs.aggregate(0)((sum, max) => sum + max._2, (sum1, sum2) => sum1 + sum2)
      println(s"max=$max")
    }
    import org.apache.spark.sql.functions._
    import spark.sqlContext.implicits._
    val maxPriceDataframe = TimeitUtils.timeIt(timeit) {
      val grouped: DataFrame = spark.read.cassandraFormat(DataLoader.Model.purchaseTable, DataLoader.Model.ks).load().select("user_id", "price").groupBy("user_id").agg(max("price") as "max")
      grouped.select(sum("max")).show()
    }

    val maxPriceDataset = TimeitUtils.timeIt(timeit) {
      val ds: sql.Dataset[Purchase1] = spark.read.cassandraFormat(DataLoader.Model.purchaseTable, DataLoader.Model.ks).load().select("user_id", "price").as[Purchase1]
      val maxs: (Long, Long) = ds.groupByKey(_.user_id).agg(typedExtended.max(_.price)).reduce((a, b) => (0, a._2 + b._2))
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

