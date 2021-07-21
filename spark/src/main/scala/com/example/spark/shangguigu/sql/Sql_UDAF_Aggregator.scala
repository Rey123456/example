package com.example.spark.shangguigu.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

object Sql_UDAF_Aggregator {

  def main(args: Array[String]): Unit = {
    //创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val df: DataFrame = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")

    //注册udf
    spark.udf.register("ageAvg", functions.udaf(new MyAvgUDAF))

    spark.sql("select ageAvg(age) from user").show()

    //关闭环境
    spark.close()
  }

  /**
    * 自定义聚合函数：求平均年龄
    * 1 继承org.apache.spark.sql.expressions.Aggregator，定义范型
    *   IN：输入数据类型
    *   BUF：缓冲区数据类型
    *   OUT：输出数据类型
    * 2 重写方法
    * */
  class MyAvgUDAF extends Aggregator[Long, Buff, Long] {

    //z & zero :初始值或零值
    //缓冲区的初始化
    override def zero: Buff = {
      Buff(0l, 0l)
    }

    //根据输入的数据更新缓冲区的数据
    override def reduce(buff: Buff, in: Long): Buff = {
      buff.total = buff.total + in
      buff.count = buff.count + 1
      buff
    }

    //缓冲区数据合并
    override def merge(buff1: Buff, buff2: Buff): Buff = {
      buff1.total = buff1.total + buff2.total
      buff1.count = buff1.count + buff2.count
      buff1
    }

    //计算结果
    override def finish(buff: Buff): Long = {
      buff.total / buff.count
    }

    //缓冲区的编码操作
    override def bufferEncoder: Encoder[Buff] = Encoders.product
    //输出的编码操作
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

  case class Buff(var total: Long, var count: Long)
}
