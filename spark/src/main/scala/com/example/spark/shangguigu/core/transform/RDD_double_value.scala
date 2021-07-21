package com.example.spark.shangguigu.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_double_value {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")//单核，结果只为一个part
    val sc = new SparkContext(sparkConf)

    /** 双value类型
      * 交、并、差要求两个数据源的数据类型保持一致
      * */
    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    val rdd2: RDD[Int] = sc.makeRDD(List(3,4,5,6))
    val rdd3: RDD[String] = sc.makeRDD(List("h", "e","l","o"))

    //交 3,4
    println(rdd1.intersection(rdd2).collect().mkString(","))

    //并 1,2,3,4,3,4,5,6
    println(rdd1.union(rdd2).collect().mkString(","))

    //差 1,2 从rdd1的角度看的差集
    println(rdd1.subtract(rdd2).collect().mkString(","))

    //拉链 (1,3),(2,4),(3,5),(4,6)
    println(rdd1.zip(rdd2).collect().mkString(","))
    //(1,h),(2,e),(3,l),(4,o) 两个数据源的数据类型可以不一致
    println(rdd1.zip(rdd3).collect().mkString(","))

    /**
      * val rdd4: RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)
      * val rdd5: RDD[Int] = sc.makeRDD(List(3,4,5,6), 4)
      * Can't zip RDDs with unequal numbers of partitions: List(2, 4)
      * 两个数据源要求分区数量保持一致
      * val rdd4: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6), 2)
      * val rdd5: RDD[Int] = sc.makeRDD(List(3,4,5,6), 2)
      * Can only zip RDDs with same number of elements in each partition
      * 两个数据源要求分区中数据数量保持一致
      * */
    val rdd4: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6), 2)
    val rdd5: RDD[Int] = sc.makeRDD(List(3,4,5,6,7,8), 2)
    println(rdd4.zip(rdd5).collect().mkString(","))


    sc.stop()
  }
}
