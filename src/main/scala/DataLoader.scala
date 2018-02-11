import com.datastax.spark.connector.cql.CassandraConnector
import com.thedeanda.lorem.{Lorem, LoremIpsum}
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import com.datastax.spark.connector._
import java.util.Random

import BenchmarkHelper.spark


object DataLoader extends App {

  def shopNumber = 100
  def usersNumber = 10000
  def usersNumber10 = usersNumber/10
  def purchaseNumberPerUser = 20

  object Model{
    val ks ="test_spark"
    val userTable = s"user_${usersNumber}"
    val userTable10 = s"user_${usersNumber10}"
    val purchaseTable = s"purchase"
    val shopTable = s"shop"
  }

  def loadData() = {
    val r = new Random(100)

    val spark = BenchmarkHelper.spark
    import spark.implicits._
    val shopNumber = 100
    val usersNumber = 10000
    val purchaseNumberPerUser = 20

    CassandraConnector(spark.sparkContext.getConf).withSessionDo(session => {
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS ${Model.ks} WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
      session.execute(s"CREATE table IF NOT EXISTS ${Model.ks}.${Model.shopTable} (shop_id bigint primary key, shop_name text, description text)")
      session.execute(s"CREATE table IF NOT EXISTS ${Model.ks}.${Model.userTable} (user_id bigint primary key, shop_id bigint, firstname text, lastname text, zipcode text, title text)")
      session.execute(s"CREATE table IF NOT EXISTS ${Model.ks}.${Model.userTable10} (user_id bigint primary key, shop_id bigint, firstname text, lastname text, zipcode text, title text)")
      session.execute(s"CREATE table IF NOT EXISTS ${Model.ks}.${Model.purchaseTable} (user_id bigint, purchase_id bigint, item text, price int, primary key ((user_id), purchase_id))")
      Seq(Model.shopTable, Model.userTable,  Model.userTable10,  Model.purchaseTable).foreach(table => {
        if(session.execute(s"select * from ${Model.ks}.$table limit 1").all().size()>0){
          session.execute(s"TRUNCATE table ${Model.ks}.$table")
        }
      })
    })

    spark.sparkContext.parallelize(1 to shopNumber).map(x => {
      val lorem = LoremIpsum.getInstance()
      Shop(x, lorem.getName(), lorem.getWords(r.nextInt(20)+5))
    }
    ).saveToCassandra(Model.ks, Model.shopTable)

    spark.sparkContext.parallelize(1 to usersNumber/10).map(x =>{
      val lorem = LoremIpsum.getInstance()
      User(x, r.nextInt(shopNumber), lorem.getFirstName(), lorem.getLastName(), lorem.getZipCode, lorem.getTitle(1))
    }
    ).saveToCassandra(Model.ks, Model.userTable10)

    spark.sparkContext.parallelize(1 to usersNumber).map(x =>{
      val lorem = LoremIpsum.getInstance()
      User(x, r.nextInt(shopNumber), lorem.getFirstName(), lorem.getLastName(), lorem.getZipCode, lorem.getTitle(1))
    }
    ).saveToCassandra(Model.ks, Model.userTable)

    spark.sparkContext.parallelize(1 to usersNumber).flatMap(user_id =>{
      val lorem = LoremIpsum.getInstance()
      (1 to r.nextInt(purchaseNumberPerUser)).map( purchase_id => Purchase(user_id, purchase_id, lorem.getWords(2, 4), r.nextInt(100)))
    }
    ).saveToCassandra(Model.ks, Model.purchaseTable)
  }

  loadData()

}

