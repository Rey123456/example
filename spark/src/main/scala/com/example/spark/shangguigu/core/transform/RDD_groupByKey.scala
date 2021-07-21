package com.example.spark.shangguigu.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_groupByKey {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")//单核，结果只为一个part
    val sc = new SparkContext(sparkConf)

    /**
      * 相同的key分在一个组中，形成一个对偶元组
      * 第一个元素是key，第二个元素是相同key的value集合
      *
      * groupByKey
      * 1. 按照key进行分组
      * 2. value会独立出来形成集合
      * groupBy:
      * 1. 不确定分组策略是什么
      * 2. 整体分组
      * */
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1), ("a", 2), ("a", 4), ("b", 5)), 2)

    val groupByKeyRdd: RDD[(String, Iterable[Int])] = rdd.groupByKey(2)
    val groupByRdd: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)

    sc.stop()
  }
}
