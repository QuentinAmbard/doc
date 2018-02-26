package tutorial

import scala.collection.{immutable, mutable}
import scala.collection.mutable.ArrayBuffer

object CollectionTutorial extends App {

  val t = new Array[Int](10)
  println(t.length)
  val t2 = Array[Int](1, 2, 3)
  println(t2.length)

  val seq: Seq[Int] = Seq(1, 2, 3)
  val list: List[Int] = seq.toList

  println("-----")
  list.foreach(i => println(i))
  println("-----")
  list.foreach(println(_))
  println("-----")
  list.foreach(println)

  println("-----")
  list.map(i => i * 2).foreach(println)
  println("-----")
  list.map(_ * 2).foreach(println)
  println("-----")

  list.flatMap(i => Seq(i,i*10)).foreach(println)

  val sum1 = list.foldLeft(0)((accumulator, i) => accumulator + i)
  val sum2 = list.sum
  val sum3 = list.reduce((a, b)=>a+b)

  assert(sum1 == sum2  && sum1 == sum3)
  println(s"sum=$sum3")

  //http://docs.scala-lang.org/overviews/collections/performance-characteristics.html
  //o(1)
  val biggerList = 1 :: list
  //o(biggerList.size)
  val evenBggerList = biggerList ::: list


  val arrayBuffer = new ArrayBuffer[Int](1)
  arrayBuffer.append(2,3)

  val immutableMap = Map("key1" -> 1)
  val map = mutable.Map("key1" -> 1)
  map("key2") = 2

  map.foreach(keyValueTuple => println(s"key=${keyValueTuple._1},value=$keyValueTuple._2"))
  print("------")
  map.foreach{case (key: String, value: Int) => println(s"key=${key},value=$value")}




}

