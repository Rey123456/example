package com.example.spark.shangguigu.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

//nc -lk 9999
object WordCount {

  def main(args: Array[String]): Unit = {
    //  创建环境对象
    // StreamingContext创建时，需要传递两个参数
    // 第一个参数表示环境配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")
    // 第二个参数表示批量处理的周期（采集周期）
    val ssc = new StreamingContext(sparkConf, Seconds(3))


    // 逻辑处理
    // 获取端口数据
    val lineStreams: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val wordsStreams: DStream[String] = lineStreams.flatMap(_.split(" "))

    val wordtoOneStreams: DStream[(String, Int)] = wordsStreams.map((_,1))

    val wordCountStreams = wordtoOneStreams.reduceByKey(_+_)

    wordCountStreams.foreachRDD(rdd => {
      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      val wordsDF = rdd.toDF("word")
      wordsDF.createOrReplaceTempView("words")
      spark.sql("select word,count(1) as total from words group by word").show()
    })

    wordCountStreams.print()

    // 由于SparkStreaming采集器是长期执行的任务，所以不能直接关闭
    // 如果main方法执行完毕，应用程序也会自动结束。所以不能让main执行完毕
    //ssc.stop()
    // 1. 启动采集器
    ssc.start()
    // 2. 等待采集器的关闭
    ssc.awaitTermination()
  }

}
