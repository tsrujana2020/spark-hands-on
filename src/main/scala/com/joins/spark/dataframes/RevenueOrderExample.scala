package com.joins.spark.dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}

case class orderItems(order_item_id:Integer,order_item_order_id:Integer,order_item_product_id:Integer,order_item_quantity:Integer,
                      order_item_subtotal:Double,order_item_product_price:Double)
object RevenueOrderExample {
  def main(args: Array[String]): Unit = {
    val spark= SparkSession.builder().master("local").appName("RevenueOrderExample").getOrCreate()
    val sqlContext=spark.sqlContext

    import spark.implicits._
    import spark.sqlContext.implicits._
    import org.apache.spark.sql.functions._

    val df= spark.read.format("csv")
                .option("header","true")
                .option("inferSchema","true")
                .csv("/Users/talas/OneDrive/Desktop/Group Links/SPARK TRAINING/ForPracticeData/orders.csv")
    val df1= spark.read.format("csv")
                     .option("header","true")
                    // .option("inferSchema","true")
                    .csv("/Users/talas/OneDrive/Desktop/Group Links/SPARK TRAINING/ForPracticeData/orderitems.csv")

   // df1.show()

    df.createOrReplaceTempView("ordersView")
    df1.createOrReplaceTempView("orderItemsView")

   /* val joinData=sqlContext.sql("select order_date from ordersView"+
      "join orderItemsView on order_id = order_item_order_id where order_status = 'COMPLETE' group by order_date")*/

    /*val JoinSelect = sqlContext.sql("Select order_date, sum(order_item_subtotal) from ordersView " +
                "join orderItemsView on order_id = order_item_order_id where order_status == 'COMPLETE' group by order_date")
                .write.option("header","true")
                .csv("C:/Users/talas/OneDrive/Desktop/Group Links/SPARK TRAINING/ForPracticeData/DFSQLJoin")*/

   // val items=sqlContext.sql("Select o.order_date, sum(oi.order_item_subtotal) from ordersView o join orderItemsView oi on o.order_id = oi.order_item_order_id ").show()


    val fJoin=sqlContext.sql("(select o.order_date, sum(oi.order_item_subtotal) revenue_per_day from " +
      "ordersView o join orderItemsView oi on o.order_id=oi.order_item_order_id where o.order_status='COMPLETE' " +
      "group by o.order_date order by order_date)")

    fJoin.show()


    /*val ordersFiltered=df.filter((df("order_status")==="COMPLETE"))
    ordersFiltered.show()*/

   // df1.show()

  }

}
