package com.scala.examples

object PatternMatchExample {
  def main(args: Array[String]): Unit = {
    val java= new Technology("Java",100)
    val scala= new Technology("Scala",200)
    val python= new Technology("Python",300)
    val R= new Technology("R",400)

    for(technology<-List(java,scala,python,R)){
      technology match {
        case Technology("Java",100)=>println("Java is printed")
        case Technology("Scala",200)=>println("Scala is printed")
        case Technology("Python",300)=>println("Python is printed")
        case Technology(name,value)=>println("name "+name+"value:"+value)
      }
    }
  }

case class Technology(name:String,value:Int)
}
