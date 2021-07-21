package com.example.spark.shangguigu.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AggentCollect {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    /**
      * 时间戳，省份，城市，用户，广告，中间字段使用空格分隔。
      * 统计出每一个省份每个广告被点击数量排行的 Top3
      * */
    //数据读入
    val dataRdd: RDD[String] = sc.textFile("datas/agent.log")

    //取需要数据
    val mapRdd = dataRdd.map(line => {
      val strings: Array[String] = line.split(" ")
      ((strings(1),strings(4)), 1)
    })
    //聚合
    val reduceByKeyRdd = mapRdd.reduceByKey(_+_)

    //结构转换
    val mapRdd1: RDD[(String, (String, Int))] = reduceByKeyRdd.map({
      case ((prv, ad), num) => (prv, (ad, num))
    })

    //按省份分组
    val groupByKeyRdd: RDD[(String, Iterable[(String, Int)])] = mapRdd1.groupByKey()

    //组内排序
    val resultRdd: RDD[(String, List[(String, Int)])] = groupByKeyRdd.mapValues({ iter => {
      iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
    }
    })

    resultRdd.collect().foreach(println)

    sc.stop()
  }
}
