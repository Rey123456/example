package com.example.spark.shangguigu.core.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object RDD_partitionBy {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")//单核，结果只为一个part
    val sc = new SparkContext(sparkConf)

    /***/
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)
    rdd.map((_,1)).partitionBy(new HashPartitioner(2)).saveAsTextFile("output")


    sc.stop()
  }
}
