package com.example.spark.shangguigu.sql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object Sql_JDBC {

  def main(args: Array[String]): Unit = {
    //创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    /**数据读取*/
    //1 通用的 load 方法读取
    spark.read.format("jdbc")
      .option("url", "jdbc:mysql://linux1:3306/spark-sql")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123123")
      .option("dbtable", "user")
      .load().show

    //2 通用的 load 方法读取 参数另一种形式
    spark.read.format("jdbc")
      .options(Map("url"->"jdbc:mysql://linux1:3306/spark-sql?user=root&password=123123",
        "dbtable"->"user","driver"->"com.mysql.jdbc.Driver")).load().show

    //3 使用 jdbc 方法读取
    val props: Properties = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "123123")
    val df: DataFrame = spark.read.jdbc("jdbc:mysql://linux1:3306/spark-sql",
      "user", props)
    df.show


    /**数据写入*/
    val rdd = spark.sparkContext.makeRDD(List(User("lisi", 20), User("zs", 30)))
    val ds: Dataset[User] = rdd.toDS
    //方式1:通用的方式 format指定写出类型
    ds.write
      .format("jdbc")
      .option("url", "jdbc:mysql://linux1:3306/spark-sql")
      .option("user", "root")
      .option("password", "123123")
      .option("dbtable", "user")
      .mode(SaveMode.Append)
      .save()
    //方式 2:通过 jdbc 方法
    ds.write.mode(SaveMode.Append)
      .jdbc("jdbc:mysql://linux1:3306/spark-sql", "user", props)

//    ds.sample()
    //关闭环境
    spark.close()
  }

  case class User(name: String, age: Long)
}
