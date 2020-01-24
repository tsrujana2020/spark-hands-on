package com.scala.examples

import org.apache.spark.sql.SparkSession

object EvenOdd {
  def main(args: Array[String]): Unit = {
   /* def isEven(number:Int)= number % 2 == 0
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
    }*/
       var spark=SparkSession.builder().master("local").appName("EvenOdd").getOrCreate()
       var sc=spark.sparkContext

      val data=sc.parallelize(1 to 20)
      val oddNum= data.filter(x=>(x%2==1))
    for(n<-oddNum){
      println("Odd Numbers count:"+n)
    }

    val evenNum=data.filter(x=>(x%2==0))
    for(n<-evenNum){
      println("Even Numbers count:"+n)
    }
  }
}
