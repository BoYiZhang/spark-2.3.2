package org.apache.sparktest

import org.apache.spark.{SparkConf, SparkContext, TaskContext}

object combineByKeyWithClassTagTest {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    conf.setAppName("WordCount").setMaster("local[*]")


    val sc = new SparkContext(conf)


    var data = sc.parallelize(List((1,1), (1,200), (1,400) , (2,1) ) ,2 )

    data.foreach(x=>{println(x+" 所属分区: "+TaskContext.get.partitionId)})


    println("===========================")

    data.combineByKeyWithClassTag[String](
      (b:Int)=>(b+5).toString,
      (k:String,v:Int)=>k+(v+1).toString,
      (v1:String,v2:String)=>v1+"#"+v2)
      .collect.foreach(println)



  }

}
