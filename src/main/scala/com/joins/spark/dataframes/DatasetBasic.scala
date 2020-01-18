package com.joins.spark.dataframes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DatasetBasic {

  def main(args: Array[String]): Unit = {
   Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("DatasetBasic").master("local").getOrCreate()

    import spark.implicits._
    val dataDS=spark.read
          .option("header","true")
          .option("inferSchema","true")
          .csv("/Users/talas/OneDrive/Desktop/Group Links/SPARK TRAINING/ForPracticeData/user_details.csv").as[Data]

    /*val filterDS=dataDS.filter(dataObj=>dataObj.user_type=="New")
    filterDS.show()
    println("Filtered Data:"+filterDS.count())*/

    val selectedCols=dataDS.select("user_name","user_type")
    selectedCols.show()
  }

}
