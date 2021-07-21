package com.example.spark.shangguigu.core.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_countByKey {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("action")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    val countByValueRes: collection.Map[Int, Long] = rdd.countByValue()
    println(countByValueRes)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a",1), ("a", 2), ("b", 5)))
    val countKeyRes: collection.Map[String, Long] = rdd1.countByKey()
    val countValueRes: collection.Map[(String, Int), Long] = rdd1.countByValue()
    println(countKeyRes)
    println(countByValueRes)


    sc.stop()
  }
}
