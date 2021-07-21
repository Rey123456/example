package com.example.spark.shangguigu.core.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_someActions {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("action")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,0))

    //reduce
    val result: Int = rdd.reduce(_+_)
    println(result)

    //collect 将不同分区的数据按照分区顺序采集到Driver端内存中，形成数组
    val collectResult: Array[Int] = rdd.collect()
    println(collectResult.mkString(","))

    //count 统计数据源中数据个数
    val cnt: Long = rdd.count()
    println(cnt)

    //first 获取数据源中的数据的第一个
    val first: Int = rdd.first()
    println(first)

    //take 获取N个数据
    val takeRes: Array[Int] = rdd.take(3)
    println(takeRes.mkString(","))

    //takeOrdered 数据排序后取N个数据
    val takeOrderedRdd: Array[Int] = rdd.takeOrdered(3)
    println(takeOrderedRdd.mkString(","))

    sc.stop()
  }
}
