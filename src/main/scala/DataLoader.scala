import com.datastax.spark.connector.cql.CassandraConnector
import com.thedeanda.lorem.Lorem
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import com.datastax.spark.connector._
import java.util.Random

import BenchmarkHelper.spark


object DataLoader extends App {

  def loadData() = {
    val ks = BenchmarkHelper.ks
    val r = new Random(100)

    val spark = BenchmarkHelper.spark
    import spark.implicits._

    CassandraConnector(spark.sparkContext.getConf).withSessionDo(session => {
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS $ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
      session.execute(s"CREATE table IF NOT EXISTS $ks.shop (shop_id bigint primary key, name text, description text)")
      session.execute(s"CREATE table IF NOT EXISTS $ks.user (user_id bigint primary key, shop_id bigint, firstname text, lastname text, zipcode text, title text)")
      session.execute(s"CREATE table IF NOT EXISTS $ks.purchase (user_id bigint, purchase_id bigint, item text, price int, primary key ((user_id), purchase_id))")
      if(session.execute(s"select * from $ks.user limit 1").all().size()>0){
        session.execute(s"TRUNCATE table $ks.purchase")
      }
      if(session.execute(s"select * from $ks.purchase limit 1").all().size()>0) {
        session.execute(s"TRUNCATE table $ks.purchase")
      }
    })

    val shopNumber = 1000
    val usersNumber = 1000000
    val purchaseNumberPerUser = 20
    spark.sparkContext.parallelize(1 to shopNumber).map(x =>
      Shop(x, Lorem.getName(), Lorem.getWords(r.nextInt(20)+5))
    ).saveToCassandra(ks, "shop")

    spark.sparkContext.parallelize(1 to usersNumber).map(x =>
      User(x, r.nextInt(shopNumber), Lorem.getFirstName(), Lorem.getLastName(), Lorem.getZipCode, Lorem.getTitle(1))
    ).saveToCassandra(ks, "user")

    spark.sparkContext.parallelize(1 to usersNumber).flatMap(user_id =>
      (1 to r.nextInt(purchaseNumberPerUser)).map( purchase_id => Purchase(user_id, purchase_id, Lorem.getWords(2, 4), r.nextInt(100)))
    ).saveToCassandra(ks, "purchase")
  }

  loadData()

}

