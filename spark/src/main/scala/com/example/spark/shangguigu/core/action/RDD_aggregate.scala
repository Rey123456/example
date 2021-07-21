package com.example.spark.shangguigu.core.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_aggregate {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("action")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)

    /** 10 + 13 + 17 = 40
      * aggregateByKey 初始值参与分区内计算
      * aggregate 初始值参与分区内、分区间计算
      * */
    val res: Int = rdd.aggregate(0)(_+_, _+_)
    println(res)

    //aggregate的简化
    val foldRes: Int = rdd.fold(0)(_+_)
    println(foldRes)

    sc.stop()
  }
}
