package tutorial

object CaseClassTutorial extends App {

  class Human(gender: String)

  trait SpeakEnglish {
    def sayHello() = println("hello")
  }

  trait SpeakFrench {
    def ditBonjour(): Unit = println("hello")
  }

  //h.gender = "aze"
  case class Person(gender: String, var name:String, val age: Int) extends Human(gender) with SpeakEnglish with SpeakFrench
  //No need of "new" : case class define an apply method as following:
  //def apply(gender: String,var name: String,val age: Int) = new Person(gender, name, age)
  val p: Person = Person("male", "quentin", 30)
  p.sayHello()
  p.ditBonjour()
  println(p.gender)
  p.name = "Jea"
  println(p.name)
  println(p.age)
  //p.age = 1 //Can't edit name

  val jumeau: Person = p.copy(name = "Jean")
  println(jumeau.toString)

}

