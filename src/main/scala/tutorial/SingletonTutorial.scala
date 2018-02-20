package tutorial

import tutorial.CaseClassTutorial.Person

import scala.util.Random


object SingletonTutorial extends App {

  object MyClass{
    val number = Random.nextInt()
  }
  class MyClass{
    def getNumber() = MyClass.number
  }

  println(MyClass.number)
  val c = new MyClass
  println(c.getNumber())
}

