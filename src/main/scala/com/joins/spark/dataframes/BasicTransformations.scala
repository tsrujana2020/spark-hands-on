package com.joins.spark.dataframes


import org.apache.spark._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object BasicTransformations {

  Logger.getLogger("org").setLevel(Level.OFF)
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local").appName("BasicTransformations").getOrCreate()
    val sc=spark.sparkContext

    //Map
   /* val data = sc.parallelize(Array("a","b","c"))
    val mapData=data.map(x=>(x,1))
    println("Normal values:"+data.collect().mkString(", "))
    println("Map values:"+mapData.collect().mkString(", "))*/

    //filter
   /* val data=sc.parallelize(Array(1,2,3,4,5,6,7,8,9,10))
    val filterData= data.filter(n=>n%2==1)
    println("Normal values:"+data.collect().mkString(", "))
    println("Filtered values:"+filterData.collect().mkString(", "))*/

    //Flat Map
   /* val data=sc.parallelize(Array(1,2,3))
    val flatMapData=data.flatMap(x=>Array(x,x*100,20))
    println("Normal values:"+data.collect().mkString(", "))
    println("Flatmap values:"+flatMapData.collect().mkString(", "))*/

    //GroupBy
   /* val data=sc.parallelize(Array("Arizona","Baltimore","California","Alabama","Alaska"))
    val groupByData=data.groupBy(x=>x.charAt(0))
    println("Values:"+data.collect().mkString(", "))
    println("Group Data"+groupByData.collect().mkString(", "))*/

    //GroupByKey
    /*val data=sc.parallelize(Array(('A',1),('B',2),('A',3),('B',5),('A',4),('B',3),('A',1)))
    val groupKeyData=data.groupByKey()
    println("Values:"+data.collect().mkString(", "))
    println("Group Data"+groupKeyData.collect().mkString(", "))*/

    //ReduceByKey and GroupByKey
    /*val words=Array("one","two","three","two","three","one","three","one","four")
    val wordsPairRDD=sc.parallelize(words).map(x=>(x,1))

    val wcWithReduce=wordsPairRDD.reduceByKey(_+_)
    println("Reduce By Key:"+wcWithReduce.collect().mkString(", "))

    val wcWithGroup=wordsPairRDD.groupByKey().map(x=>(x._1,x._2.sum))
    println("groupByKey:"+wcWithGroup.collect().mkString(", "))*/

    //MapPartition
    /*val data=sc.parallelize(Array(1,2,3),2)
    def f(i:Iterator[Int])={
      (i.sum,100).productIterator
    }
    val mapPartData=data.mapPartitions(f)
    val dataOut=data.glom()
    println("Data:"+data.collect().mkString(", "))
    val mapPartOut=mapPartData.glom()
    println("MapPartitions:"+mapPartData.collect().mkString(", "))*/

    //MapPartitionWithIndex
   /* val data=sc.parallelize(Array(1,2,3),2)
    def f(partitionIndex:Int, i:Iterator[Int])={
      (partitionIndex,i.sum).productIterator
    }
    val withIndex=data.mapPartitionsWithIndex(f)
    println("With index:"+withIndex.collect().mkString(", "))*/

    //SAMPLE
    /*val data= sc.parallelize(Array(1,2,3,4,5))
    val sampleData=data.sample(false,0.3)
    println("Sample Data:"+sampleData.collect().mkString(", "))*/

    //UNION
   /* val data=sc.parallelize(Array(1,2,3),2)
    val data1=sc.parallelize(Array(3,4),1)
    val resData=data.union(data1)
    println("Union Data:"+resData.collect().mkString(", "))*/

    //Coalesce
   /* val data=sc.parallelize(Array(1,2,3,4,5),3)
    val coalesceData=data.coalesce(2)
    println("data:"+data.glom().collect())
    println("Coalesce:"+coalesceData.glom().collect())*/

    //KeyBy
   /* val data=sc.parallelize(Array("Arizona","Baltimore","California","Alabama","Alaska"))
    val keyByData=data.keyBy(x=>x.charAt(0))
    println("Values:"+data.collect().mkString(", "))
    println("Group Data"+keyByData.collect().mkString(", "))*/

    //partitionBY
    val data=sc.parallelize(Array(('A',"Arizona"),('B',"Baltimore"),('C',"California"),('A',"Alabama")),3)


  }

}
