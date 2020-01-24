package com.spark.examples

import org.apache.spark
import org.apache.spark._
import org.apache.spark.sql.SparkSession

object WordCount {

  def main(args: Array[String]) {
    val conf =new spark.SparkConf().setAppName("WordCount").setMaster("local[1]")
     val sc= new SparkContext(conf)
    /*val conf= new SparkConf()

    conf.set("spark.master", args(0))
    conf.set("spark.app.name", args(1))

    val sc=new SparkContext(conf)
    sc.setLogLevel("WARN")
    val r1=sc.textFile(args(2))
    val r2= r1.flatMap(_.split(' ')).map((_,1)).reduceByKey(_+_)
    r2.saveAsTextFile(args(3))
    sc.stop()*/

   val data = sc.textFile("/Users/talas/OneDrive/Desktop/Group Links/SPARK TRAINING/ForPracticeData/sampleData.txt")
   val result = data.flatMap(_.split(' ')).map((_,1)).reduceByKey((_+_))

   val output=result.saveAsTextFile("/Users/talas/OneDrive/Desktop/Group Links/SPARK TRAINING/ForPracticeData/OUTPUT.txt")

println("Word count:"+result.collect())


  }

}
