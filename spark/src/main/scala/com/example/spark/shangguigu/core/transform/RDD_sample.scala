package com.example.spark.shangguigu.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_sample {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")//单核，结果只为一个part
    val sc = new SparkContext(sparkConf)


    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7,8,9,10))
    /**
      * 抽取数据不放回(伯努利算法)
      * 伯努利算法:又叫 0、1 分布。
      * 具体实现:根据种子和随机算法算出一个数和第二个参数设置几率比较，小于第二个参数要，大于不要
      * 第一个参数:抽取的数据是否放回，false:不放回
      * 第二个参数:抽取的几率，范围在[0,1]之间,0:全不取;1:全取;表示每条数据被抽取的概率
      * 第三个参数:随机数种子，默认使用当前系统时间*/
    rdd.sample(false, 0.4, 1).foreach(println)
    /**
      *抽取数据放回(泊松算法)
      * 第一个参数:抽取的数据是否放回，true:放回;false:不放回
      * 第二个参数:重复数据的几率，范围大于等于0.表示每一个元素被期望抽取到的次数
      * 第三个参数:随机数种子
      * */
    rdd.sample(true, 0.4, 1).foreach(println)


    sc.stop()
  }
}
