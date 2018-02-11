import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._


object BenchmarkBrodcastJoins extends App {

  val spark =  SparkSession.builder
    .master("local[2]")
    .appName("testStructured")
    .config("spark.executor.memory", "1G")
    .config("spark.cassandra.connection.factory", "CustomConnectionFactory")
    //Disable broadcast joins for tests
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .getOrCreate()
  import spark.implicits._


  def getData(userTable: String, shopTable: String): Unit = {
    //Disable broadcast join
    val count = spark.sparkContext.cassandraTable(DataLoader.Model.ks, userTable).joinWithCassandraTable(DataLoader.Model.ks, shopTable).count()
    println("Spark small Dataframe join")
    val userJoinShopDFBroadcast = TimeitUtils.timeIt(1) {
      val shop = spark.read.cassandraFormat(DataLoader.Model.shopTable, DataLoader.Model.ks).load().as[Shop]
      val users = spark.read.cassandraFormat(userTable, DataLoader.Model.ks).load().as[User]
      //Force a broadcast join, will override conf threshold. Current limit to 2GB (block size, SPARK-6235)
      import org.apache.spark.sql.functions.broadcast
      val join = users.join(broadcast(shop), "shop_id")
      println(join.explain())
      assert(count == join.count())
    }
    val userJoinShopDFNoBroadcast = TimeitUtils.timeIt(1) {
      val shop = spark.read.cassandraFormat(DataLoader.Model.shopTable, DataLoader.Model.ks).load().as[Shop]
      val users = spark.read.cassandraFormat(userTable, DataLoader.Model.ks).load().as[User]
      val join = users.join(shop, "shop_id")
      println(join.explain())
      assert(count == join.count())
    }
    val broadcastDF = Dataset("Spark small Dataframe join", Seq(userJoinShopDFNoBroadcast, userJoinShopDFBroadcast))


    println("Spark small RDD join")
    val userJoinShopRDDBroadcast = TimeitUtils.timeIt(1) {
      //Start by collecting the small RDD and broadcast it:
      val shops = spark.sparkContext.cassandraTable[Shop](DataLoader.Model.ks, shopTable).keyBy(s => s.shop_id).collect().toMap
      val broadcastShops = spark.sparkContext.broadcast(shops)
      val users = spark.sparkContext.cassandraTable[User](DataLoader.Model.ks, userTable)
      val join: RDD[(User, Shop)] = users.flatMap(user => {
        broadcastShops.value.get(user.shop_id).map {shop => (user, shop)}
      })
      println(join.toDebugString)
      assert(count == join.count())
    }
    val userJoinShopRDDBNoroadcast = TimeitUtils.timeIt(1) {
      val users = spark.sparkContext.cassandraTable[User](DataLoader.Model.ks, userTable)
      val joins = users.joinWithCassandraTable[Shop](DataLoader.Model.ks, shopTable, joinColumns = SomeColumns("shop_id"))
      println(joins.toDebugString)
      assert(count == joins.count())
    }
    val broadcastRDD = Dataset("Spark small RDD join", Seq(userJoinShopRDDBroadcast, userJoinShopRDDBNoroadcast))

    val measure = Measure(Seq(s"$userTable => $shopTable Broadcast", s"$userTable => $shopTable"), Seq(broadcastDF, broadcastRDD))

    println(Json.mapper.writeValueAsString(measure))
    Json.mapper.writeValueAsString(measure)

  }


  getData(DataLoader.Model.userTable, DataLoader.Model.purchaseTable)
  getData(DataLoader.Model.userTable10, DataLoader.Model.shopTable)

}

