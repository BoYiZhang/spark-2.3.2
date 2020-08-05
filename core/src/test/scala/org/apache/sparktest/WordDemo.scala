package org.apache.sparktest

import org.apache.spark.{SparkConf, SparkContext}

object WordDemo {

  //spark-submit --name 'WordDemo'  --class WordDemo --master yarn --num-executors 1 --executor-memory 1G --executor-cores 1 spark-test.jar /mysqlClean.sql

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage:SparkWordCount FileName")
      System.exit(1)
    }
    val conf = new SparkConf();
    conf.setAppName("WordCount")

    println("===================== Stage 0   start   ====================  ")
    val sc = new SparkContext(conf)
    val zrdd1 = sc.textFile(args(0))
    val zrdd2 = zrdd1.flatMap(line => line.split(" "))
    val zrdd3 =zrdd2.map(word => (word, 1))
    println("===================== Stage 0   end   =====================  ")

    println("===================== Stage 1   start   ====================  ")
    val zrdd4 =zrdd3.reduceByKey(_ + _)

    val zrdd5_7 =zrdd4.map(x => {
      (x._1, 1)
    })
    println("===================== Stage 1   end   ====================  ")


    println("===================== Stage 2   start   ====================  ")

    println("zrdd4 会被 依赖到这里 , 加入stage2 。。。")
    val zrdd6_8 = zrdd4.map(x => {
      (x._1, 2)
    })
    println("===================== Stage 2   end    ====================  ")


    println("===================== Stage 3   start   ====================  ")

    println("zrdd5， zrdd6  会被 依赖到这里 , 加入stage3 。。。")
    val zrdd9 = zrdd5_7.leftOuterJoin( zrdd6_8 )

    val zrdd10 = zrdd9.map( x => {
      (x._1, 1)
    })

    val data = Array("A","B","C","D")
    val zrdd11 = sc.parallelize(data)

    val zrdd12 = zrdd11.map(x => {
      ( x ,1)
    })

    val zrdd13 = zrdd10.union(zrdd12)

    val zrdd14 = zrdd13.map(x => {
      (x._1 , 1)
    })
    println("===================== Stage 3   end    ====================  ")
    println(" result  : " + zrdd14.count())

  }

}

