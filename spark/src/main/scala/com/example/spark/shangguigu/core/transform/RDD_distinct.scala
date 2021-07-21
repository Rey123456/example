package com.example.spark.shangguigu.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_distinct {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")//单核，结果只为一个part
    val sc = new SparkContext(sparkConf)

    /** 如何distinct
      * map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)
      * */
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,1,2,3,4,5,3,2))
    rdd.distinct().foreach(println)

    /**scala distinct 利用hashset*/
    List(1,2,1,2).distinct.foreach(println)

    sc.stop()
  }
}
