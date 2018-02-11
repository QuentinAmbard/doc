import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector
import com.datastax.spark.connector.rdd.{CassandraJoinRDD, CassandraTableScanRDD}


object BenchmarkJoins extends App {

  import com.datastax.spark.connector.rdd.CassandraTableScanRDD._

  val spark = BenchmarkHelper.spark
  import spark.implicits._

  def getData(userTable: String, purchaseTable: String): Unit = {
    val count = spark.sparkContext.cassandraTable(DataLoader.Model.ks, purchaseTable).joinWithCassandraTable(DataLoader.Model.ks, userTable).count()
    val joinWithCassandraTableData: Dataset = joinWithCassandra(count, userTable, purchaseTable)
    val joinKeyByData = joinByKey(count, userTable, purchaseTable)
    val joinKeyBySamePartitionerData = joinKeyBySamePartitioner(count, userTable, purchaseTable)
    val joinSpanBySamePartitionerData: Dataset = joinByKeySpanBySamePartitioner(count, userTable, purchaseTable)
    val sparkJoinDSData: Dataset = sparkDSJoin(count, userTable, purchaseTable)
    val sparkJoinDFData: Dataset = sparkDFJoin(count, userTable, purchaseTable)

    val measure = Measure(Seq(s"$userTable => $purchaseTable", s"$purchaseTable => $userTable"), Seq(joinWithCassandraTableData,
      joinKeyByData, joinKeyBySamePartitionerData, joinSpanBySamePartitionerData, sparkJoinDSData, sparkJoinDFData))

    println(Json.mapper.writeValueAsString(measure))
    Json.mapper.writeValueAsString(measure)
  }

  private def sparkDFJoin(count: Long, userTable: String, purchaseTable: String) = {
    println("Spark Dataframe Join")
    val userJoinPurchaseDF = TimeitUtils.timeIt(1) {
      val purchases = spark.read.cassandraFormat(purchaseTable, DataLoader.Model.ks).load()
      val users = spark.read.cassandraFormat(userTable, DataLoader.Model.ks).load()
      val joinDF = users.joinWith(purchases, purchases("user_id") === users("user_id"))
      println(joinDF.explain)
      assert(count == joinDF.count())
    }
    val purchaseJoinUserDF = TimeitUtils.timeIt(1) {
      val purchases = spark.read.cassandraFormat(purchaseTable, DataLoader.Model.ks).load()
      val users = spark.read.cassandraFormat(userTable, DataLoader.Model.ks).load()
      val joinDF = purchases.joinWith(users, purchases("user_id") === users("user_id"))
      println(joinDF.explain)
      assert(count == joinDF.count())
    }
    Dataset("Spark Dataframe Join", Seq(userJoinPurchaseDF, purchaseJoinUserDF))
  }

  private def sparkDSJoin(count: Long, userTable: String, purchaseTable: String) = {
    println("Spark Dataset Join")
    val userJoinPurchaseDS = TimeitUtils.timeIt(1) {
      val purchases = spark.read.cassandraFormat(purchaseTable, DataLoader.Model.ks).load().as[Purchase]
      val users = spark.read.cassandraFormat(userTable, DataLoader.Model.ks).load().as[User]
      val joinDS = users.joinWith(purchases, purchases("user_id") === users("user_id"))
      println(joinDS.explain)
      assert(count == joinDS.count())
    }
    val purchaseJoinUserDS = TimeitUtils.timeIt(1) {
      val purchases = spark.read.cassandraFormat(purchaseTable, DataLoader.Model.ks).load().as[Purchase]
      val users = spark.read.cassandraFormat(userTable, DataLoader.Model.ks).load().as[User]
      val joinDS = purchases.joinWith(users, purchases("user_id") === users("user_id"))
      println(joinDS.explain)
      assert(count == joinDS.count())
    }
    Dataset("Spark Dataset Join", Seq(userJoinPurchaseDS, purchaseJoinUserDS))
  }

  private def joinByKeySpanBySamePartitioner(count: Long, userTable: String, purchaseTable: String) = {
    println("RDD join span by key")
    val userJoinPurchaseSamePartitionerSpan = TimeitUtils.timeIt(1) {
      val purchases = spark.sparkContext.cassandraTable(DataLoader.Model.ks, purchaseTable).spanBy(r => r.getLong("user_id"))
      val users = spark.sparkContext.cassandraTable(DataLoader.Model.ks, userTable).keyBy(r => r.getLong("user_id"))
      val joinrdd = purchases.join(users)
      println(joinrdd.toDebugString)
      assert(count == joinrdd.aggregate(0L)((count, row) => count + row._2._1.size, (count1, count2) => count1 + count2))
    }
    val purchaseJoinUserSamePartitionerSpan = TimeitUtils.timeIt(1) {
      val purchases= spark.sparkContext.cassandraTable(DataLoader.Model.ks, purchaseTable).spanBy(r => r.getLong("user_id"))
      val users = spark.sparkContext.cassandraTable(DataLoader.Model.ks, userTable).keyBy(r => r.getLong("user_id"))
      val joinrdd = purchases.join(users)
      println(joinrdd.toDebugString)
      assert(count == joinrdd.aggregate(0L)((count, row) => count + row._2._1.size, (count1, count2) => count1 + count2))
    }
    Dataset("RDD join span by key", Seq(userJoinPurchaseSamePartitionerSpan, purchaseJoinUserSamePartitionerSpan))
  }


  private def joinWithCassandraTableSpanByKey(count: Long, userTable: String, purchaseTable: String) = {
    println("RDD join span by key")
    val purchaseJoinUserCassandraTableSpanByKey = TimeitUtils.timeIt(1) {
      val joinrdd = spark.sparkContext.cassandraTable(DataLoader.Model.ks, purchaseTable).spanBy(r => r.getLong("user_id")).joinWithCassandraTable(DataLoader.Model.ks, userTable)
      assert(count == joinrdd.aggregate(0L)((count, row) => count + row._1._2.size, (count1, count2) => count1 + count2))
    }
    Dataset("RDD join span by key", Seq(0, purchaseJoinUserCassandraTableSpanByKey))
  }




  private def joinKeyBySamePartitioner(count: Long, userTable: String, purchaseTable: String) = {
    println("RDD join keyBy same partitioner")
    val userJoinPurchaseSamePartitionerKey = TimeitUtils.timeIt(1) {
      val purchases = spark.sparkContext.cassandraTable(DataLoader.Model.ks, purchaseTable).keyBy[Tuple1[Long]]("user_id")
      val users = spark.sparkContext.cassandraTable(DataLoader.Model.ks, userTable).keyBy[Tuple1[Long]]("user_id").applyPartitionerFrom(purchases)
      val joinrdd = purchases.join(users)
      println(joinrdd.toDebugString)
      assert(count == joinrdd.count())
    }
    val purchaseJoinUserSamePartitionerKey = TimeitUtils.timeIt(1) {
      val users = spark.sparkContext.cassandraTable(DataLoader.Model.ks, userTable).keyBy[Tuple1[Long]]("user_id")
      val purchases = spark.sparkContext.cassandraTable(DataLoader.Model.ks, purchaseTable).keyBy[Tuple1[Long]]("user_id").applyPartitionerFrom(users)
      val joinrdd = purchases.join(users)
      println(joinrdd.toDebugString)
      assert(count == joinrdd.count())
    }
    Dataset("RDD join keyBy same partitioners", Seq(userJoinPurchaseSamePartitionerKey, purchaseJoinUserSamePartitionerKey))
  }

  private def joinByKey(count: Long, userTable: String, purchaseTable: String) = {
    println("RDD join keyBy")
    val userJoinPurchaseKeyBy = TimeitUtils.timeIt(1) {
      val purchases = spark.sparkContext.cassandraTable(DataLoader.Model.ks, purchaseTable).keyBy(r => r.getLong("user_id"))
      val users = spark.sparkContext.cassandraTable(DataLoader.Model.ks, userTable).keyBy(r => r.getLong("user_id"))
      val joinrdd = purchases.join(users)
      println(joinrdd.toDebugString)
      assert(count == joinrdd.count())
    }
    val purchaseJoinUserKeyBy = TimeitUtils.timeIt(1) {
      val purchases = spark.sparkContext.cassandraTable(DataLoader.Model.ks, purchaseTable).keyBy(r => r.getLong("user_id"))
      val users = spark.sparkContext.cassandraTable(DataLoader.Model.ks, userTable).keyBy(r => r.getLong("user_id"))
      val joinrdd = purchases.join(users)
      println(joinrdd.toDebugString)
      assert(count == joinrdd.count())
    }
    Dataset("RDD join keyBy", Seq(userJoinPurchaseKeyBy, purchaseJoinUserKeyBy))
  }

  private def joinWithCassandra(count: Long, userTable: String, purchaseTable: String) = {
    println("joinWithCassandraTable")
    val userJoinPurchase = TimeitUtils.timeIt(1) {
      val joinRdd = spark.sparkContext.cassandraTable(DataLoader.Model.ks, userTable).joinWithCassandraTable(DataLoader.Model.ks, purchaseTable)
      println(joinRdd.toDebugString)
      assert(count == joinRdd.count())
    }
    val purchaseJoinUser = TimeitUtils.timeIt(1) {
      val joinRdd = spark.sparkContext.cassandraTable(DataLoader.Model.ks, purchaseTable).joinWithCassandraTable(DataLoader.Model.ks, userTable)
      println(joinRdd.toDebugString)
      assert(count == joinRdd.count())
      joinRdd.foreach(println)
    }
    Dataset("joinWithCassandra", Seq(userJoinPurchase, purchaseJoinUser))
  }


  private def joinWithCassandraMapper(count: Long, userTable: String, purchaseTable: String) = {
    println("joinWithCassandraTable")
    val userJoinPurchase = TimeitUtils.timeIt(1) {
      val joinRdd = spark.sparkContext.cassandraTable[User](DataLoader.Model.ks, userTable).joinWithCassandraTable[Purchase](DataLoader.Model.ks, purchaseTable)
      println(joinRdd.toDebugString)
      assert(count == joinRdd.count())
    }
    val purchaseJoinUser = TimeitUtils.timeIt(1) {
      val joinRdd = spark.sparkContext.cassandraTable[Purchase](DataLoader.Model.ks, purchaseTable).joinWithCassandraTable[User](DataLoader.Model.ks, userTable)
      println(joinRdd.toDebugString)
      assert(count == joinRdd.count())
      joinRdd.foreach(println)
    }
    Dataset("joinWithCassandra Mapper", Seq(userJoinPurchase, purchaseJoinUser))
  }

  //getData(DataLoader.Model.userTable, DataLoader.Model.purchaseTable)
  getData(DataLoader.Model.userTable10, DataLoader.Model.purchaseTable)

}

