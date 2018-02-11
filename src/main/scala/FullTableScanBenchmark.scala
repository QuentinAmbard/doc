import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cassandra._


object FullTableScanBenchmark extends App {

  val spark = BenchmarkHelper.spark
  import spark.implicits._

  def getData(table: String): Unit = {
    val timeitCount = 3
    val rddTime = TimeitUtils.timeIt(timeitCount) {
      spark.sparkContext.cassandraTable[Purchase](DataLoader.Model.ks, table).map(f => (f.user_id,f.item,f.price,f.purchase_id)).count()
    }
    val rddTimeNoMapper = TimeitUtils.timeIt(timeitCount) {
      spark.sparkContext.cassandraTable(DataLoader.Model.ks, table).map(f => (f.getLong("user_id"),f.getString("item"),f.getInt("price"),f.getLong("purchase_id"))).count()
    }
    val dstime = TimeitUtils.timeIt(timeitCount) {
      spark.read.cassandraFormat(table, DataLoader.Model.ks).load().as[Purchase].map(f => (f.user_id,f.item,f.price,f.purchase_id)).count()
    }
    val dftime = TimeitUtils.timeIt(timeitCount) {
      spark.read.cassandraFormat(table, DataLoader.Model.ks).load().map(r=>(r.getLong(0),r.getLong(1),r.getString(2),r.getInt(3))).count()
    }
    val measure = Measure(Seq("Full Table Scan"), Seq(Dataset("RDD scan mapper", Seq(rddTime)),
      Dataset("RDD scan", Seq(rddTimeNoMapper)),
      Dataset("DF scan", Seq(dftime)),
      Dataset("DS scan", Seq(dstime))))
    println(Json.mapper.writeValueAsString(measure))
  }

  getData(DataLoader.Model.purchaseTable)

}

