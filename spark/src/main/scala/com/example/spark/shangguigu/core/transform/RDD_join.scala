package com.example.spark.shangguigu.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_join {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")//单核，结果只为一个part
    val sc = new SparkContext(sparkConf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a",1), ("b", 2), ("c", 3), ("a", 4), ("d", 5)))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a",1), ("b", 2), ("c", 3), ("a", 4)))

    /** 两个不同的数据源，相同key的value会连接在一起，形成元组
       如果两个数据源中key没有匹配上，则不会在结果中出现
       如果两个数据源中key有多个相同的，会依次匹配，可能会出现笛卡尔乘积，数据量会几何性增长，会导致性能降低，内存溢出。
      * */
    val joinRdd: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
    joinRdd.collect().foreach(println)

    val rdd3: RDD[(String, Int)] = sc.makeRDD(List(("a",1), ("b", 2), ("c", 3), ("d", 4)))
    val rdd4: RDD[(String, Int)] = sc.makeRDD(List(("a",4), ("b", 5), ("c", 6), ("e", 4)))

    val leftOuterJoinRdd: RDD[(String, (Int, Option[Int]))] = rdd3.leftOuterJoin(rdd4)
    val rightOuterJoinRdd: RDD[(String, (Option[Int], Int))] = rdd3.rightOuterJoin(rdd4)
    val fullOuterJoinRdd: RDD[(String, (Option[Int], Option[Int]))] = rdd3.fullOuterJoin(rdd4)
    leftOuterJoinRdd.collect().foreach(println)
    rightOuterJoinRdd.collect().foreach(println)
    fullOuterJoinRdd.collect().foreach(println)

    sc.stop()
  }
}
