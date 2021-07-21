package com.example.spark.shangguigu.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_glom {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")//单核，结果只为一个part
    val sc = new SparkContext(sparkConf)

    /**将分区的数据输出*/
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)
    val glomRdd: RDD[Array[Int]] = rdd.glom()
    glomRdd.collect().foreach(data => println(data.mkString(",")))

    /**计算所有分区最大值求和(分区内取最大值，分区间最大值求和)*/
    val maxRdd: RDD[Int] = glomRdd.map(array => array.max)
    println(maxRdd.collect().sum)

    sc.stop()
  }
}
