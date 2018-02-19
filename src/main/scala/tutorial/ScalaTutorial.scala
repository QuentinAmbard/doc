package tutorial


object ScalaTutorial extends App {

  val immutable = 3
  //immutable = 5 //Doesn't work
  var mutable = 3
  mutable = 4

  println("immutable="+immutable)
  println(s"mutable=$mutable")

  val l: Long = 3L
  val i: AnyVal = 3 //value (primitive)
  val b: Any = false //parent class of all values (AnyVal)/object (AnyRef)

}

