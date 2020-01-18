package com.joins.spark.dataframes

import org.apache.spark.sql.SparkSession
//import com.databricks.spark.avro

object AvroFileFormat {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("AvroFile").master("local").getOrCreate()

    val avrDF= spark.read.format("avro")
                  .load("C:/Users/talas/OneDrive/Desktop/Group Links/SPARK TRAINING/ForPracticeData/episodes.avro")

    avrDF.printSchema()
    avrDF.show(5)
    avrDF.count()
  }
}
