package tutorial

import tutorial.CaseClassTutorial.Person

object OptionTutorial extends App {

  val empty: Option[Long] = None
  println(s"empty.isEmpty=${empty.isEmpty}, empty.isDefined=${empty.isDefined}")

  val defined = Some(1L)
  println(s"empty.isEmpty=${empty.isEmpty}, empty.isDefined=${empty.isDefined}")

  def getValue(option: Option[Long]) = option match {
    case None => "empty"
    case Some(value) => s"value=$value"
  }

  println(getValue(empty))
  println(getValue(defined))
  println("getOrElse")
  println(empty.getOrElse(0))
  println(defined.getOrElse(0))
  println("map")
  println(getValue(empty.map(l => l*100)))
  println(getValue(defined.map(l => l*100)))
  println("fold")
  println(empty.fold(5L)(i => i*10))
  println(defined.fold(5L)(i => i*10))

}

