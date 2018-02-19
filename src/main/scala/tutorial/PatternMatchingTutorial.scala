package tutorial

import tutorial.CaseClassTutorial.{Human, Person}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object PatternMatchingTutorial extends App {

  val t = 10

  val result = t match {
    case 10 => "I'm 10"
    case _ => "I'm everything expect 10"
  }
  println(result)

  println(3 match {
    case 10 => "I'm 10"
    case i if i > 5 => "I'm not 10 but i'm > 5"
    case _ => "I'm everything expect 10"
  })

  val person: Person = CaseClassTutorial.Person("male", "quentin", 30)
  println(person match {
    case p: Person => "I'm a person"
    case _ => "I'm not a person"
  })

  //Useful for recursive functions
  List(1, 2, 3) match {
    case Nil => println("empty list")
    case (head: Int) :: tail => println(s"$head :: $tail")
  }

}

