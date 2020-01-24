package com.joins.spark.dataframes

import org.apache.spark.sql.SparkSession

case class Data1(transaction_id:Integer,user_name:String,user_type:String,transaction_date:String)
object TempView {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TempView").master("local").getOrCreate()
    val sc= spark.sparkContext

    import spark.implicits._
    val dsData=spark.read.option("header","true").option("inferSchema",true)
      .csv("/Users/talas/OneDrive/Desktop/Group Links/SPARK TRAINING/ForPracticeData/user_details.csv").as[Data1]

  /*  val datasetData=Seq(1,2,3,4,5).toDS()
    datasetData.show()*/
    dsData.printSchema()
    dsData.show()
    dsData.count()
  }


}
