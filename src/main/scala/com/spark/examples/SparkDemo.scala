package com.spark.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.Row

object SparkDemo {
  def main(args: Array[String]): Unit = {
    val spark= SparkSession.builder().appName("SparkDemo").master("local[*]").getOrCreate()
    val sc= spark.sparkContext

    val data = Array("1","2","3","4","5")
    val rdd= sc.parallelize(data)

    val schema=StructType(
      StructField("Integers as Strings:",StringType,true)::Nil
    )
    val rowRDD = rdd.map(x=>Row(x))
    val df = spark.createDataFrame(rowRDD,schema)

    df.printSchema()
    df.show()


  }
}
