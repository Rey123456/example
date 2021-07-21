package com.example.spark.shangguigu.core.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_foreach {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("action")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)

    //Driver端内存集合的循环遍历方法
    rdd.collect().foreach(println)
    println("*******************")
    //Executor端内存数据打印
    rdd.foreach(println)

    /**
      算子：operator
      rdd方法和scala集合的方法不一样
      集合对象的方法都是在同一个节点的内存中完成的
      RDD的方法可以将计算逻辑发送到Executor端执行
      RDD方法外部的操作都是在Driver端执行的（比如上述的println("*******")），而方法内部的逻辑代码是在Executor端执行。
      * */

    sc.stop()
  }
}
