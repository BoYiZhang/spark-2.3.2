package org.apache.sparktest

import org.apache.spark.{SparkConf, SparkContext}

object CombineByKeyTest {


  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()

    conf.setAppName("WordCount").setMaster("local[*]")


    val sc = new SparkContext(conf)


    var rdd1 = sc.makeRDD(Array(("A",1),("A",2),("B",11),("B",12),("C",111)))


//    ================  step 1 start    ==========================


    //    val data = rdd1.combineByKey(
//             (v : Int) => {
//              val data =  v + "_"
//               println(" v1 : " + data )
//               data
//             },
//           (c : String, v : Int) => {
//             val data =  c + "@" + v
//             println(" v2 : " + data )
//             data
//           },
//           (c1 : String, c2 : String) => {
//             val data =  c1 + "$" + c2
//             println(" v3 : " + data )
//             data
//           }
//         ).collect
//
//    data.foreach(println)

//    v1 : 12_
//    v1 : 2_
//    v1 : 1_
//    v1 : 11_
//    v1 : 111_
//    v3 : 1_$2_
//      v3 : 11_$12_
//      (A,1_$2_)
//    (B,11_$12_)
//    (C,111_)


//    ================  step 1 end    ==========================
    val data = rdd1.combineByKey(
      (v : Int) => {
        val data = List(v)
        println(" v1 : " + data )
        data
      },
      (c : List[Int], v : Int) => {

        val data = v :: c
        println(" v2 : " + data )
        data
      },
      (c1 : List[Int], c2 : List[Int]) => {

        val data = c1 ::: c2
        println(" v3 : " + data )
        data
      }
    ).collect

    data.foreach(println)




//    ================  step 2 start    ==========================





//    ================  step 2 end    ==========================




    //    case class Juice(volumn: Int) {
//      def add(j: Juice):Juice = Juice(volumn + j.volumn)
//    }
//    case class Fruit(kind: String, weight: Int) {
//      def makeJuice:Juice = Juice(weight * 100)
//    }
//    val apple1 = Fruit("apple", 5)
//    val apple2 = Fruit("apple", 8)
//    val orange1 = Fruit("orange", 10)
//
//    val fruit = sc.parallelize(List(("apple", apple1) , ("orange", orange1) , ("apple", apple2)))
//    val juice = fruit.combineByKey(
//      f => f.makeJuice,
//      (j:Juice, f) => j.add(f.makeJuice),
//      (j1:Juice, j2:Juice) => j1.add(j2)
//    )
//
//    juice.foreach(println)


//    (orange,Juice(1000))
//    (apple,Juice(1300))

  }


}
