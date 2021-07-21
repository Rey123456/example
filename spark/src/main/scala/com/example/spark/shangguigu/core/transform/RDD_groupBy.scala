package com.example.spark.shangguigu.core.transform

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_groupBy {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")//单核，结果只为一个part
    val sc = new SparkContext(sparkConf)

    /**奇偶分组*/
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)
    val groupRdd: RDD[(Int, Iterable[Int])] = rdd.groupBy(_%2)
    groupRdd.collect().foreach(println)

    /**List("Hello", "hive", "hbase", "Hadoop")根据单词首写字母进行分组*/
    val rdd1: RDD[String] =
      sc.makeRDD(List("Hello", "hive", "hbase", "Hadoop"), 2)
    val groupRdd1: RDD[(Char, Iterable[String])] = rdd1.groupBy(_.charAt(0))
    groupRdd1.collect().foreach(println)

    /**从服务器日志数据apache.log中获取每个时间段(只有小时)访问量。*/
    val rdd2: RDD[String] = sc.textFile("datas/apache.log")
    //83.149.9.216 - - 17/05/2015:10:05:56 +0000 GET /favicon.ico
    val groupRdd2: RDD[(String, Iterable[String])] = rdd2.groupBy(str => {
      val time: String = str.split(" ")(3)
      val date: Date = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss").parse(time)
      new SimpleDateFormat("HH").format(date)
    })
    groupRdd2.foreach(
      kv => println(kv._1, kv._2.size)
    )

    sc.stop()
  }
}
