package com.joins.spark.dataframes

import org.apache.spark.sql.SparkSession

case class Data(transaction_id:Integer,user_name:String,user_type:String,transaction_date:String)
object SparkDS extends App {

  val spark=SparkSession.builder().appName("SparkDS").master("local").getOrCreate()

  import spark.implicits._

  val dataDS = spark.read.option("header","true").option("inferSchema",true)
    .csv("/Users/talas/OneDrive/Desktop/Group Links/SPARK TRAINING/ForPracticeData/user_details.csv").as[Data]

  dataDS.printSchema()
  dataDS.count()
  dataDS.show()

}
