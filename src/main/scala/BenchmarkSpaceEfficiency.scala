import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._


object BenchmarkSpaceEfficiency {

  val spark = BenchmarkHelper.spark
  import spark.implicits._
  def main(args: Array[String]): Unit = {
    spark.sparkContext.cassandraTable[Purchase](DataLoader.Model.ks, DataLoader.Model.purchaseTable).cache().count()
    //1110.7 MB
    spark.sparkContext.cassandraTable(DataLoader.Model.ks, DataLoader.Model.purchaseTable).cache().count()
    //1110.7 MB
    spark.read.cassandraFormat(DataLoader.Model.purchaseTable, DataLoader.Model.ks).load().cache().count()
    //231.8MB
    spark.read.cassandraFormat(DataLoader.Model.purchaseTable, DataLoader.Model.ks).load().as[Purchase].cache().count()
    //231.8MB
  }

}

