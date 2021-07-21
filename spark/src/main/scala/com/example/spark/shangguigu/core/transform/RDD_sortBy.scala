package com.example.spark.shangguigu.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_sortBy {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")//单核，结果只为一个part
    val sc = new SparkContext(sparkConf)

    /**sortBy
      * 根据指定的规则对数据源中的数据进行排序操作，升/降取决于第二个参数，默认升序
      * 默认情况下不会改变分区，但中间存在shuffle操作
      * */
    val rdd: RDD[Int] = sc.makeRDD(List(1,7,3,2,5,6,4),2)
    rdd.sortBy(num => num).saveAsTextFile("output")
    rdd.sortBy(num => num,true,1).saveAsTextFile("output1")//改变分区

    val rdd1: RDD[(String, Int)] =
      sc.makeRDD(List(("1", 1),("11", 2),("2", 3)), 2)
    rdd1.sortBy(_._1).collect().foreach(println)
    rdd1.sortBy(_._1.toInt).collect().foreach(println)
    rdd1.sortBy(_._1.toInt, false).collect().foreach(println)

    sc.stop()
  }
}
