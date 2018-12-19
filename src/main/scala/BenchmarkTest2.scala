import scala.util.Random


object BenchmarkTest2 {

  val spark = BenchmarkHelper.spark

  var timeit = 1

  def main(args: Array[String]) = {
    if (args.length > 0) timeit = args(0).toInt
    println("RDD join span by key")
    import AsynchRDDHelper._
    val t = spark.sparkContext.parallelize(1 to 10000).mapPartitionAsynch(it => {
      it.mapAsync(v => {
        Thread.sleep(Random.nextInt(10))
        -1
      })
    })
    println(t.filter(_<0).count())

//    val test = spark.sparkContext.parallelize(Seq(new TestJAva("", "", "aze", new TestJAva2("a")), new TestJAva("", "", "e", new TestJAva2("a")), new TestJAva("", "", "z", new TestJAva2("a"))))
//    import org.apache.spark.sql.Encoders
//    implicit val myEncoder = Encoders.kryo(classOf[TestJAva])
//    val ds = spark.createDataset(test)
//    ds.foreach(f => println(f))
//    ds.toDF().show()

    spark.stop()
  }
}

