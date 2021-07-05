package com.zl ;


import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import scala.Function1;
import scala.Tuple2;

import java.io.File;


public class SparkShuffleFileTest {
  public static void main(String[] args) {
    String resPath = "/opt/a/spark_test/res";
    File res = new File(resPath);
    res.deleteOnExit();

    SparkConf conf = new SparkConf().setAppName("app").setMaster("local[4]");

    conf.set("spark.shuffle.spill.numElementsForceSpillThreshold","1000");

    JavaSparkContext sc  = new JavaSparkContext(conf);

    String path = "/opt/a/spark_test/shuffle.txt" ;

    JavaRDD<String> textFile = sc.textFile(path,4);
    JavaPairRDD<String,Integer> data = textFile.mapToPair(s -> new Tuple2<String,Integer>(s, 1)).reduceByKey((a,b) -> a + b);

    data.groupByKey().saveAsTextFile("/opt/a/spark_test/res");


  }

}
