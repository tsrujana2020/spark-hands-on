package com.spark.examples

import org.apache.spark.sql.SparkSession

object BroadcastExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("BroadcastExample").master("local").getOrCreate()
    val sc= spark.sparkContext

    val para = ("This video is a part of Spark tutorial and spark interview Question. " +
                 "My xeample is in Scala but similarly u can do in Python").split(" ")

    val lookupTable = Map("Spark"->500,"Scala"->300,"Python"->225,"Hive"->328,"Sqoop"->290)

    val bigDataTerms=para.map(x=>lookupTable.getOrElse(x,8))
    bigDataTerms.foreach(println)

    val broadCastLookup = sc.broadcast(lookupTable)
    val bidDataLookup=para.map(x=>broadCastLookup.value.getOrElse(x,0))
    bidDataLookup.foreach(println)
    broadCastLookup.destroy();


  }

}
