package com.example.spark.shangguigu.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Sql_UDF {

  def main(args: Array[String]): Unit = {
    //创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val df: DataFrame = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")

    //注册udf
    spark.udf.register("prefixName", (name:String) => {
      "Name:" + name
    })

    spark.sql("select prefixName(username) as name, age from user").show()

    //关闭环境
    spark.close()
  }
}
