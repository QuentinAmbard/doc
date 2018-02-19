package tutorial

object ClassTutorial extends App {

  class Human(gender: String)

  trait SpeakEnglish {
    def sayHello() = println("hello")
  }

  trait SpeakFrench {
    def ditBonjour() = println("hello")
  }

  class Person(gender: String, var name:String, val age: Int = 30) extends Human(gender) with SpeakEnglish with SpeakFrench {
    def this(name:String) = this("male", name)
  }

  val p1 = new Person("quentin")
  val p = new Person("male", "quentin", 30)
  println(p)
  p.sayHello()
  p.ditBonjour()
  //p.gender //can't access gender
  p.name = "Jea"
  println(p.name)
  println(p.age)
  //p.age = 1 //Can't edit name

  val human: Human = p.asInstanceOf[Human]


}

