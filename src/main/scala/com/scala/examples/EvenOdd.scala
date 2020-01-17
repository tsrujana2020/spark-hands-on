package com.scala.examples

object EvenOdd {
  def main(args: Array[String]): Unit = {
    def isEven(number:Int)= number % 2 == 0
    def isOdd(number:Int)= !isEven(number)

    val numList=List.range(0,10)
    for(n<-numList){
      print(n)
      if(isEven(n)){
        println("Even number:")
      }
      if(isOdd(n)){
        println("Odd Number:")
      }
    }
  }
}
