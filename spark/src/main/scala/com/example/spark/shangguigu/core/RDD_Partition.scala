package com.example.spark.shangguigu.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Partition {

  def main(args: Array[String]): Unit = {
    /** [*]当前系统的最大可用核数，不写为单核
      * */
    //val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD") [*]下并行度为8
    val sparkConf = new SparkConf().setMaster("local").setAppName("RDD")//单核，结果只为一个part
    val sc = new SparkContext(sparkConf)

    //分区数为2
    //val rdd: RDD[Int] = sc.parallelize(List(1,2,3,4),2)
    /**
      * 分区数可以不传，使用默认值：defaultParallelism
      * scheduler.conf.getInt("spark.default.parallelism", totalCores)
      * totalCores:当前运行环境的最大可用核数
      * */
    val rdd = sc.parallelize(List(1,2,3,4))
    rdd.saveAsTextFile("output")

    /**minPartitions :最小分区数
      * 默认值 def defaultMinPartitions: Int = math.min(defaultParallelism, 2)
      *
      * 使用FileInputFormat中的getSplits函数进行分区 totalSize
      * long goalSize = totalSize / (long)(numSplits == 0 ? 1 : numSplits); 每个分区的数据字节大小
      * 当 总大小/goalSize后不能整除时 会再多出一个分区
      * */

    /** 数据如何划分
      * 当1.txt中存放内容为：（@表示特殊字符，包括回车、换行）
      * 内容 => 偏移量
        1@@ => 012
        2@@ => 345
        3   => 6

    1. 数据以行为单位进行读取：spark读取文件，采用hadoop的方式读取，所以是一行一行读取，与字节数没有关系
    2. 数据读取时是以偏移量为单位，偏移量不会被重复读取
    3. 数据分区的偏移量范围的计算
    0 => [0, 3]
    1 => [3, 6]
    2 => [6, 7]
    【1 2】【3】【】
      * */
    val rdd1: RDD[String] = sc.textFile("datas/2.txt", 2)
    rdd1.saveAsTextFile("output1")

    /**
      * 数据内容:
      1234567@@ => 0123456789
      89@@  => 10 11 12 13
      0     => 14

      * totalSize = 14
      * goalSize = 14/2 = 7 能整除，2个分片
      *
      * 行读 + 偏移量不会被重复
      * 【1234567】
      * 【89
      *   0】
      * */
    val rdd2: RDD[String] = sc.textFile("datas/3.txt",2)
    rdd2.saveAsTextFile("output2")

    /**如果数据源是多个文件，那么计算分区时是以文件为单位进行分区
      * 文件只能切分不能合并，不然还要CombineInputFormat干啥
      * */
    val rdd3: RDD[String] = sc.textFile("datas", 5)
    rdd3.saveAsTextFile("output3")

    sc.stop()
  }
}
