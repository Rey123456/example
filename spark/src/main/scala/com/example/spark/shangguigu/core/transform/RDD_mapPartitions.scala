package com.example.spark.shangguigu.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_mapPartitions {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")//单核，结果只为一个part
    val sc = new SparkContext(sparkConf)

    /**mapPartitions， 类似于缓存的感觉，数据缓存一部分后集中处理，加快速度
      * 以分区为单位进行数据转换
      * 但会将整个分区的数据加载到内存中进行引用
      * 处理完的部分数据是不会被释放掉的，因为存在引用
      * 在内存较小，数据量较大的情况下会出现内存溢出
      * */
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)
    val mapRdd: RDD[Int] = rdd.mapPartitions(iter => {
      println(">>>>>>>>")
      iter.map(_ * 2)
    })
    mapRdd.collect().foreach(println)

    /**寻找分区内的最大值*/
    val mapRdd1: RDD[Int] = rdd.mapPartitions(
      iter => {
        List(iter.max).iterator
      }
    )
    mapRdd1.collect().foreach(println)

    sc.stop()
  }
}
