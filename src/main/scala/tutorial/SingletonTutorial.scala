package tutorial

import tutorial.CaseClassTutorial.Person

import scala.util.Random

object MyClass{
  val number = Random.nextInt()
}
class MyClass{
  def getNumber() = MyClass.number
}
object SingletonTutorial extends App {

  println(MyClass.number)
  val c = new MyClass
  println(c.getNumber())
}

