package com.example.spark.shangguigu.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Sql_Test {

  def main(args: Array[String]): Unit = {
    //创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //spark.sql("select age,from_unixtime(unix_timestamp(date_format('2021032023','yyyyMMddHH'))-3600 * 33 ) from json.`datas/user.json`").show()
    spark.sql("select age,from_unixtime(1616556707,'yyyyMMddHH') from json.`datas/user.json`").show()


    //关闭环境
    spark.close()
  }

  case class User(id: Int, username: String, age: Int)
}
