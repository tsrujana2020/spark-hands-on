package com.spark.examples

import org.apache.spark.sql.SparkSession

object DFWithCsv {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("DFWithCsv").master("local").getOrCreate()

    val properties= Map("header"->"true","inferSchema"->"true")
    val df=spark.read
                .options(properties)
                .csv("C:/Users/talas/OneDrive/Desktop/Group Links/SPARK TRAINING/ForPracticeData/user_details.csv")

    //df.selectExpr("_c0 as TXID","_c1 as UserName","_c2 as UserType","_c3 as TxDate").show()
    df.show()
  }

}
