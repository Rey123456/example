package com.example.spark.shangguigu.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_aggregateByKey {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")//单核，结果只为一个part
    val sc = new SparkContext(sparkConf)

    /**
      * 分区内最大值，分区间求和
      *
      * aggregateByKey 存在函数柯里化，有两个参数列表
      * 第一个参数列表，需要传递一个参数，表示为初始值(相同key的初始值)
      *       主要用于碰见第一个key时，和value进行分区内计算
      * 第二个参数列表需要传递两个参数
      *       第一个参数表示：分区内计算规则
      *       第二个参数表示：分区间计算规则
      * */
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)), 2)

    val aggregateByKeyRdd: RDD[(String, Int)] =
      rdd.aggregateByKey(0)((x, y) => math.max(x, y), (x, y) => (x + y))

    aggregateByKeyRdd.collect().foreach(println)

    /**分区内计算规则和分区间计算规则相同时，aggregateByKey 就可以简化为 foldByKey
      * */
    val foldByKeyRdd: RDD[(String, Int)] = rdd.foldByKey(0)(_+_)
    foldByKeyRdd.collect().foreach(println)

    /** aggregateByKey最终的返回数据结果应该和初始值类型保持一致
      * 获取相同key的数据平均值 (数量的总和，次数)
      * */
    val aggRdd: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0,0))(
      (t, v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1, t2) => {(t1._1 + t2._1, t1._2 + t2._2)}
    )
    val resultRdd: RDD[(String, Int)] = aggRdd.mapValues({
      case (num, cnt) => {
        num / cnt
      }
    })
    resultRdd.collect().foreach(println)

    /** combineByKey
      * 参数1：将相同key的第一个数据进行结构的转换，实现操作
      * 参数2：分区内的计算规则
      * 参数3：分区间的计算规则
      * 获取相同key的数据平均值 (数量的总和，次数)
      * */
    val combineByKeyRdd: RDD[(String, (Int, Int))] = rdd.combineByKey(
      v => (v, 1),
      (t:(Int, Int), v) => {(t._1 + v, t._2 + 1)},
      (t1:(Int, Int), t2:(Int, Int)) => {(t1._1 + t2._1, t1._2 + t2._2)}
    )
    combineByKeyRdd.mapValues({
      case (num, cnt) => {
        num / cnt
      }
    }).collect().foreach(println)

    sc.stop()
  }
}
