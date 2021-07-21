package com.example.spark.shangguigu.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_cogroup {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")//单核，结果只为一个part
    val sc = new SparkContext(sparkConf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a",1), ("b", 2), ("c", 3), ("a", 4), ("d", 5)))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a",1), ("b", 2), ("c", 3), ("a", 4)))

    /**分组+连接
      * */
    val cogroupRdd: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)
    cogroupRdd.collect().foreach(println)

    sc.stop()
  }
}
