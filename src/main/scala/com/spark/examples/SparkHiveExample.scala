package com.spark.examples

import java.io.File

import org.apache.spark.sql.SparkSession

object SparkHiveExample {

  case class Record(key:Int,value:String)
  def main(args: Array[String]): Unit = {

    val warehouseLocation=new File("Spark-warehouse").getAbsolutePath


   /* val spark = SparkSession.builder().appName("SparkHiveExample").config("spark.sql.warehouse.dir",warehouseLocation)
                                      .enableHiveSupport().getOrCreate()*/
   val spark = SparkSession

     .builder()

     .appName("SparkHiveExample")

     .config("spark.sql.warehouse.dir", warehouseLocation)

     .enableHiveSupport()

     .getOrCreate()

    import spark.implicits._
    import spark.sql

    sql("CREATE table if not exists src(key:INT,value:STRING) using hive" )
    sql("LOAD DATA LOCAL INPATH '/Users/talas/OneDrive/Desktop/Group Links/SPARK TRAINING/ForPracticeData/employee.txt' INTO TABLE src")

    sql("select * from src").show()


  }


}
