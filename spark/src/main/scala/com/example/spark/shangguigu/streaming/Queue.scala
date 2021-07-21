package com.example.spark.shangguigu.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object Queue {

  def main(args: Array[String]): Unit = {
    //  创建环境对象
    // StreamingContext创建时，需要传递两个参数
    // 第一个参数表示环境配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")
    // 第二个参数表示批量处理的周期（采集周期）
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //创建 RDD 队列, 创建 QueueInputDStream
    val rddQueue = new mutable.Queue[RDD[Int]]()
    val inputStream = ssc.queueStream(rddQueue, oneAtATime = false)

    val resultStream: DStream[(Int, Int)] = inputStream.map((_,1)).reduceByKey(_+_)

    resultStream.print()

    ssc.start()

    //循环创建并向 RDD 队列中放入 RDD
    for (i <- 1 to 5){
      rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
      Thread.sleep(2000)
    }

    ssc.awaitTermination()
  }

}
