package com.example.spark.shangguigu.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_mapPartitionsWithIndex {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")//单核，结果只为一个part
    val sc = new SparkContext(sparkConf)

    /**获取第二个分区的数据*/
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)
    val mpiRdd: RDD[Int] = rdd.mapPartitionsWithIndex((index, iter) => {
      if (index == 1) iter else Nil.iterator //空的迭代器
    })
    mpiRdd.collect().foreach(println)

    /**获取每个数据所在分区*/
    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4,5))
    val mpiRdd1: RDD[(Int, Int)] = rdd1.mapPartitionsWithIndex((index, iter) => {
      iter.map((index, _))
    })
    mpiRdd1.collect().foreach(println)

    sc.stop()
  }
}
