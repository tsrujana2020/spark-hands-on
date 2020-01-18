package com.joins.spark.dataframes


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession

object SparkDF {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val spark= SparkSession.builder().appName("SparkDF").master("local[*]").getOrCreate()
    import spark.implicits._

    val df1=spark.read.format("csv")
                      .option("header","true")
                      .option("inferSchema",true)
                      .load("/Users/talas/OneDrive/Desktop/Group Links/SPARK TRAINING/ForPracticeData/user_details.csv")

    val addCol=df1.withColumn("Year",$"transaction_date".substr(7,4))
    addCol.createOrReplaceTempView("addColViewTable")

    //read the lookup file
    val df2= spark.read.format("csv")
                        .option("header","true")
                        .option("inferschema",true)
                        .load("C:/Users/talas/OneDrive/Desktop/Group Links/SPARK TRAINING/ForPracticeData/valid_year.csv")
    df2.createOrReplaceTempView("df2ViewTable")

    spark.sqlContext.sql("select transaction_id,user_name,user_type,transaction_date from" +
            "(select * from addColViewTable a left join df2ViewTable b ON a.Year=b.valid_year)"+"where valid_year IS NULL")
            .write.option("header","true").csv("C:/Users/talas/OneDrive/Desktop/Group Links/SPARK TRAINING/ForPracticeData/REJECT_OUTPUT")

    spark.sqlContext.sql("select transaction_id,user_name,user_type,transaction_date from" +
      "(select * from addColViewTable a left join df2ViewTable b ON a.Year=b.valid_year)"+"where valid_year IS NOT NULL")
      .write.option("header","true").csv("C:/Users/talas/OneDrive/Desktop/Group Links/SPARK TRAINING/ForPracticeData/VALID_OUTPUT")
  }

}
