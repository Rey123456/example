package com.example.spark.shangguigu.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_map_Test {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")//单核，结果只为一个part
    val sc = new SparkContext(sparkConf)

    /**map
      * 从服务器日志数据apache.log中获取用户请求URL资源路径
      * */
    val rdd: RDD[String] = sc.textFile("datas/apache.log")
    //83.149.9.216 - - 17/05/2015:10:05:56 +0000 GET /favicon.ico
    val mapRdd: RDD[String] = rdd.map(line => {
      val datas: Array[String] = line.split(" ")
      datas(datas.length-1)
    })
    mapRdd.collect().foreach(println)

    sc.stop()
  }
}
