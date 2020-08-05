package org.apache

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
object WordCount {


  // 启动脚本：
  // spark-submit --name spark-test --class WordCount --master yarn --deploy-mode cluster /A/spark-test.jar /A/readme.rd



  def main(args: Array[String]): Unit = {


    if (args.length < 1) {
      println("Usage:SparkWordCount FileName")
      System.exit(1)
    }


    val conf = new SparkConf()

    conf.setAppName("WordCount")
//          .set("spark.master","yarn")
//          .set("spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_URI_BASES","http://bj-rack001-hadoop003:8088")
//          .set("spark.ui.port","0")
//          .set("spark.app.name","Spark Exercise: Spark Version Word Count Program")
//          .set("spark.submit.deployMode","cluster")
//          .set("spark.ui.filters","org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter")
//          .set("spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_HOSTS","bj-rack001-hadoop003")
//          .set("spark.yarn.app.id","application_1545118153810_0000")

//      .setMaster("yarn")
      .setMaster("local[*]")


    val sc = new SparkContext(conf)
    
    println("===================== Stage 0   start =====================  ")
    
    val zrdd1 = sc.textFile(args(0))
    val zrdd2 = zrdd1.flatMap(line => line.split(" "))
    val zrdd3 =zrdd2.map(word => {
      (word, 1)
    })


    println("===================== Stage 0   end   =====================  ")


    
    
    println("===================== Stage 1   start =====================  ")
    val zrdd4 =zrdd3.reduceByKey(_ + _)

    val zrdd5 =zrdd4.map(x => {
      ("@"+x._1, 100)
    })
    println("===================== Stage 1   end   =====================  ")

    zrdd4.collectAsMap()


    println("===================== Stage 2   start =====================  ")

    val zrrd6 = zrdd5.reduceByKey(_ + _ )

    val zrdd7 =zrrd6.map(x => {
      ("@"+x._1, 1)
    })
    println("===================== Stage 2   end   =====================  ")


    val zrrd8 = zrdd7.reduceByKey(_ + _ ).map(x => {
      ("@"+x._1, 1)
    })
    val zrrd9 = zrrd8.reduceByKey(_ + _ ).map(x => {
      ("@"+x._1, 1)
    })
    val zrrd10 = zrrd9.reduceByKey(_ + _ ).map(x => {
      ("@"+x._1, 1)
    })

    println(" result  : " + zrrd10.count())


  }


}

