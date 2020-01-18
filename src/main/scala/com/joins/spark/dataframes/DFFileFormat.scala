package com.joins.spark.dataframes

import org.apache.spark.sql.SparkSession

object DFFileFormat {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DFFileFormat").master("local").getOrCreate()

    val jsonDF=spark.read.json("/Users/talas/OneDrive/Desktop/Group Links/SPARK TRAINING/ForPracticeData/people.json")
    jsonDF.printSchema()
    jsonDF.show()
    println("Count of json DF::"+jsonDF.count())

    val orcDF=spark.read.orc("/Users/talas/OneDrive/Desktop/Group Links/SPARK TRAINING/ForPracticeData/ORDDATA.orc")
    orcDF.printSchema()
    orcDF.show()
    println("Count of orc DF:"+orcDF.count())

    val parquetDF=spark.read.parquet("/Users/talas/OneDrive/Desktop/Group Links/SPARK TRAINING/ForPracticeData/users.parquet")
    parquetDF.printSchema()
    parquetDF.show()
    println("Count of parquet DF:"+parquetDF.count())
  }
}
