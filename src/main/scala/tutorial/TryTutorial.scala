package tutorial

import scala.util.{Failure, Random, Success, Try}

object TryTutorial extends App {

  def getValue[A](myTry: Try[A]) = myTry match {
    case Success(value) => s"success=$value"
    case Failure(exception) => s"exception=$exception"
  }

  println(getValue(Try(12)))
  println(getValue(Try("hello")))
  println(getValue(Try(throw new RuntimeException("oooops"))))

  Try(12).map(i => println(i))
  val i = 10
  println("map")
  Try(if(i>0) throw new RuntimeException("oooops") else 0).map(i => println(i))
  Try(if(i<0) throw new RuntimeException("oooops") else 0).map(i => println(i))
}

