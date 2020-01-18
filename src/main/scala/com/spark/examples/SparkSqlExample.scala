package com.spark.examples

import org.apache.spark.sql.SparkSession

case class People(age:Int,name:String)
object SparkSqlExample {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("SparkSqlExample").master("local").getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._
    val jsonDF=spark.read.json("/Users/talas/OneDrive/Desktop/Group Links/SPARK TRAINING/ForPracticeData/people.json")

   // jsonDF.show()
    /*jsonDF.printSchema()
    jsonDF.select("name").show()
    jsonDF.select($"name",$"age"+5).show()
    jsonDF.filter($"age">20).show()
    jsonDF.groupBy("age").count().show()*/
    jsonDF.createOrReplaceTempView("People")

    val sqlDF=spark.sql("select * from People")
    sqlDF.show()

    val caseClassDF=Seq(People(29,"Tommy")).toDS()
    caseClassDF.show()

    val primitiveDS=Seq(1,2,3,4,5).toDS()
    primitiveDS.map(_+1).show()

  }

}
