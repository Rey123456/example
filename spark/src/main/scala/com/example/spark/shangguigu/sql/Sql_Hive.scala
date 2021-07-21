package com.example.spark.shangguigu.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Sql_Hive {

  def main(args: Array[String]): Unit = {
    //创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate() //.enableHiveSupport()

    /** 使用sparksql连接外部的Hive
      * 1 拷贝Hive-size.xml文件到classpath下
      * 2 启用Hive的支持 .enableHiveSupport()
      * 3 增加对应的依赖关系(包含MySQL驱动)
      * */
    spark.sql("show tables").show()

    //关闭环境
    spark.close()
  }

  case class User(name: String, age: Long)
}
