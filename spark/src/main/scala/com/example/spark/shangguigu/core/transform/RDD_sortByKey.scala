package com.example.spark.shangguigu.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_sortByKey {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")//单核，结果只为一个part
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)), 2)

    val sortByKeyRdd: RDD[(String, Int)] = rdd.sortByKey()
    sortByKeyRdd.collect().foreach(println)

    sc.stop()
  }
}
