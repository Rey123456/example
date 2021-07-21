package com.example.spark.shangguigu.structuredstreaming

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object example {

  def main(args: Array[String]): Unit = {
    //1. 定义SparkSession
    val spark = SparkSession.builder().appName("stryctStreaming").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    //2. 定义DataFarme,使用spark.readStream监听TCP端口数据并实时转为DataFarme
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()
    //3. 定义操作
    val words = lines.as[String].flatMap(_.split(" "))
    val wordCount = words.groupBy("value").count()
    wordCount.printSchema()

    val query = wordCount.writeStream
      .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
      .outputMode("complete")
      .format("console")
      .start() //4. 启动 StreamingQuery 实时流计算
    query.awaitTermination()
  }
}
