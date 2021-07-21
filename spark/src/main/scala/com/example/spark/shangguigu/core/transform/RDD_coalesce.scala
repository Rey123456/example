package com.example.spark.shangguigu.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_coalesce {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")//单核，结果只为一个part
    val sc = new SparkContext(sparkConf)

    /**coalesce*/
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),4)
    rdd.coalesce(2).saveAsTextFile("output")

    /**rdd1 [1,2] [3,4] [5,6]
      * coalesce后：【1,2】 【3,4,5,6】
      * 默认情况下不会将分区的数据打乱重新组合，可能会导致数据不均衡，出现数据倾斜
      * 如果想要数据均衡，可以进行shuffle处理，即第二个参数选择true
      * */
    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6), 3)
    rdd1.coalesce(2).saveAsTextFile("output1")

    rdd1.coalesce(2, true).saveAsTextFile("output2")

    /**扩大分区
      * coalesce也可以扩大分区，但一定要选择shuffle，不然没有意义，因为分区数据不会被打乱拆分
      * 缩减分区：coalesce，若想要数据均衡，需选择shuffle
      * 扩大分区：repartition，调用的就是shuffle的coalesce
      * */
    rdd1.coalesce(3, true).saveAsTextFile("output3")
    rdd1.repartition(3).saveAsTextFile("output4")


    sc.stop()
  }
}
