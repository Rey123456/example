package com.example.spark.shangguigu.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object RDD_reduceByKey {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")//单核，结果只为一个part
    val sc = new SparkContext(sparkConf)

    /**
      * 聚合操作是两两聚合的
      * reduceByKey中如果key的数据只有一个，是不会参与运算的
      * */
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1), ("a", 2), ("a", 4), ("b", 5)), 2)

    val reducebykeyRdd: RDD[(String, Int)] = rdd.reduceByKey((x: Int, y: Int) => {
      println(s"x=${x}, y=${y}")
      x + y
    })
    reducebykeyRdd.collect().foreach(println)


    sc.stop()
  }
}
