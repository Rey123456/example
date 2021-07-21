package com.example.spark.shangguigu.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Sql_Basic {

  def main(args: Array[String]): Unit = {
    //创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //DataFrame
    val df: DataFrame = spark.read.json("datas/user.json")
    df.show()
    //DataFrame => sql
    df.createOrReplaceTempView("user") //临时的表视图

    spark.sql("select * from user").show()
    spark.sql("select username,age from user").show()
    spark.sql("select avg(age) from user").show()

    //DataFrame => DSL
    //在使用DataFrame时如果涉及到转换操作，需要引入转换规则 import spark.implicits._
    df.select("username", "age").show()
    df.select($"age" + 1).show()
    df.select('age + 1).show()

    //DataFrame 其实是特定范型的DataSet
    val seq = Seq(1, 2, 3, 4)
    val ds: Dataset[Int] = seq.toDS()
    ds.show()

    //RDD <=> DataFrame
    val rdd: RDD[(Int, String, Int)] =
      spark.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 40)))
    val df1: DataFrame = rdd.toDF("id", "username", "age")
    val rowRDD: RDD[Row] = df1.rdd

    //DataFrame <=> DataSet
    val ds1: Dataset[User] = df1.as[User]
    val df2: DataFrame = ds1.toDF()

    //RDD <=> DataSet
    val ds2: Dataset[User] = rdd
      .map {
        case (id, username, age) => {
          User(id, username, age)
        }
      }
      .toDS()
    val UserRdd: RDD[User] = ds2.rdd

    //关闭环境
    spark.close()
  }

  case class User(id: Int, username: String, age: Int)
}
