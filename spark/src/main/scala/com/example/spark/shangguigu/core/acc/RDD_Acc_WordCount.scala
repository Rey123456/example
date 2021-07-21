package com.example.spark.shangguigu.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable

object RDD_Acc_WordCount {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("Acc")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(List("hello", "spark", "hello"))
    //rdd.map((_,1)).reduceByKey(_+_)

    //累加器 WordCount
    //创建累加器对象
    val accumulator: MyAccumulator = new MyAccumulator()
    //向spark进行注册
    sc.register(accumulator, "wordCount")


    rdd.foreach(
      word => {
        //累加
        accumulator.add(word)
      }
    )

    println(accumulator.value)

    sc.stop()
  }

  /**自定义数据累加器 WordCount
    1. 继承AccumulatorV2，定义范型
      IN：累加器输入数据类型
      OUT：累加器返回数据类型
    2. 重写方法
    */
  class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]]{
    private val wcMap = mutable.Map[String, Long]()
    //判断是否为初始状态
    override def isZero: Boolean = {
      wcMap.isEmpty
    }
    // 复制累加器
    override def copy(): AccumulatorV2[
      String,
      mutable.Map[String, Long]
    ] = {
      new MyAccumulator()
    }
    // 重置累加器
    override def reset(): Unit = {
      wcMap.clear()
    }

    //获取累加器需要计算的值
    override def add(v: String): Unit = {
      val newCount = wcMap.getOrElse(v, 0l) + 1
      wcMap.update(v, newCount)
    }

    //driver合并累加器
    override def merge(
      other: AccumulatorV2[
        String,
        mutable.Map[String, Long]
      ]
    ): Unit = {
        other.value.foreach{
          case (word, count) => {
            val newCount = wcMap.getOrElse(word, 0l) + count
            wcMap.update(word, newCount)
          }
        }
    }

    //累加器结果
    override def value
      : mutable.Map[String, Long] = wcMap
  }

}
