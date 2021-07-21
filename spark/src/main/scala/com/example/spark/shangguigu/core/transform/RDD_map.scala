package com.example.spark.shangguigu.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_map {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("transform")//单核，结果只为一个part
    val sc = new SparkContext(sparkConf)

    /**map
      * */
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    //转换函数
    def mapFunction(num : Int) : Int ={
      num * 2
    }

//    val mapRdd: RDD[Int] = rdd.map(mapFunction)//声明函数
//    val mapRdd: RDD[Int] = rdd.map((num:Int)=>{num*2})//匿名函数
//    val mapRdd: RDD[Int] = rdd.map((num:Int)=>num*2)//至简原则，函数代码只有一行时花括号可以省略
//    val mapRdd: RDD[Int] = rdd.map((num)=>num*2)//至简原则，参数类型可以自动推断出来，类型可以省略
//    val mapRdd: RDD[Int] = rdd.map(num=>num*2)//至简原则，参数类型只有一个，小括号可以省略
    val mapRdd: RDD[Int] = rdd.map(_*2)//至简原则，参数在逻辑中只出现一次且按顺序，可以用_代替
    mapRdd.foreach(println)

    sc.stop()
  }
}
