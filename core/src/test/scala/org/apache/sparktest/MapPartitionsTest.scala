package org.apache.sparktest

import org.apache.spark.{SparkConf, SparkContext}

object MapPartitionsTest {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("MapPartitionsTest").setMaster("local[*]")


    val sc = new SparkContext(conf)


    //生成10个元素3个分区的rdd a，元素值为1~10的整数（1 2 3 4 5 6 7 8 9 10），sc为SparkContext对象

    val a = sc.parallelize(1 to 1000000, 3)

    //定义两个输入变换函数，它们的作用均是将rdd a中的元素值翻倍

    //map的输入函数，其参数e为rdd元素值

    def myfuncPerElement(e:Int):Int = {

//      println("e="+e)

      e*2

    }

    //mapPartitions的输入函数。iter是分区中元素的迭代子，返回类型也要是迭代子

    def myfuncPerPartition ( iter : Iterator [Int] ) : Iterator [Int] = {

//      println("run in partition")

      var res = for (e <- iter ) yield e*2

      res

    }


    var startTime = System.currentTimeMillis() ;

//    val b = a.map(myfuncPerElement).collect

    var endTime = System.currentTimeMillis() ;

    println(" ******************************** map total use :  " + (endTime -startTime)  )



    startTime = System.currentTimeMillis() ;


    val c =  a.mapPartitions(myfuncPerPartition).collect
    endTime = System.currentTimeMillis() ;


    println(" ******************************** mapPartitions  total use :  " + (endTime -startTime)  )

  }


}
