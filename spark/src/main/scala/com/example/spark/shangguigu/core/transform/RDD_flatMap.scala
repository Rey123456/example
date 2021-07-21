package com.example.spark.shangguigu.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_flatMap {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")//单核，结果只为一个part
    val sc = new SparkContext(sparkConf)

    /**将二维数据转为一维数组*/
    val rdd: RDD[List[Int]] = sc.makeRDD(List(List(1,2),List(3,4)))
    val flatRdd: RDD[Int] = rdd.flatMap(iter => iter)
    flatRdd.collect().foreach(println)

    /**将字符串按空格切分成单词*/
    val rdd1: RDD[String] = sc.makeRDD(List("hello world", "hello scala"))
    val flatRdd1: RDD[String] = rdd1.flatMap(str => str.split(" "))
    flatRdd1.collect().foreach(println)

    /**将 List(List(1,2),3,List(4,5))进行扁平化操作*/
    val rdd2: RDD[Any] = sc.makeRDD(List(List(1,2),3,List(4,5)))
    val flatRdd2: RDD[Any] = rdd2.flatMap(data => {
      data match {
        case list: List[_] => list
        case dat           => List(dat)
      }
    })
    flatRdd2.collect().foreach(println)

    sc.stop()
  }
}
