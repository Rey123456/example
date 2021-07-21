package com.example.spark.shangguigu.core.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

object Rdd_Partition {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String, String)] = sc.makeRDD(
      List(
      ("nba", "*******"),
      ("cba", "*******"),
      ("nba", "*******"),
      ("wnba", "*******")
      )
    )
    rdd.partitionBy(new MyPartition(3)).saveAsTextFile("output")

    sc.stop()
  }

  /**自定义分区器
    * 1 继承Partitioner
    * 2 重写方法
    * */
  class MyPartition(partitions: Int) extends Partitioner {
    //分区数
    override def numPartitions: Int = partitions
    //返回数据的分区索引
    override def getPartition(key: Any): Int = {
      key match {
        case "nba" => 0
        case "cba" => 1
        case _ => 2
      }
    }
  }
}
