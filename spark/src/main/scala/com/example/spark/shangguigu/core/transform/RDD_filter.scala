package com.example.spark.shangguigu.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_filter {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")//单核，结果只为一个part
    val sc = new SparkContext(sparkConf)

    /**过滤偶数*/
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    val filterRdd: RDD[Int] = rdd.filter(_%2!=0)
    filterRdd.collect().foreach(println)

    /**从服务器日志数据apache.log中获取2015年5月17日的用户请求URL资源路径*/
    val rdd1: RDD[String] = sc.textFile("datas/apache.log")
    //83.149.9.216 - - 17/05/2015:10:05:56 +0000 GET /favicon.ico
    val filterRdd1: RDD[String] =
      rdd1.filter(!_.contains("17/05/2015")).map(_.split(" ")(6))
    filterRdd1.collect().foreach(println)

    sc.stop()
  }
}
