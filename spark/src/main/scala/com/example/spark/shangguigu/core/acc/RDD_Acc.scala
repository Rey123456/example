package com.example.spark.shangguigu.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Acc {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("Acc")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    //获取累加器 spark默认提供了简单数据聚合的累加器
    val sumAcc: LongAccumulator = sc.longAccumulator
    rdd.foreach(num =>
    //使用累加器
      sumAcc.add(num))

    //获取值
    //当累加器在转换算子中时，可能因为转换算子的多次执行或者没有action导致累加器多加或少加
    //所以一般情况下，累加器都放在执行算子中
    println(sumAcc.value)

    sc.stop()
  }

}
