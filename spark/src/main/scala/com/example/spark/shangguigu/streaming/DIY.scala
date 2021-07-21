package com.example.spark.shangguigu.streaming

import java.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DIY {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val messageDS: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver)
    messageDS.print()

    ssc.start()
    ssc.awaitTermination()
  }

  /** 自定义数据采集器
    1. 继承Receiver，定义泛型, 传递参数
    2. 重写方法
    * */
  class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY){
    private var flg = true
    override def onStart(): Unit = {
      new Thread(new Runnable {
        override def run(): Unit = {
          while (flg){
            val message = new Random().nextInt(10).toString
            store(message)
            Thread.sleep(500)
          }
        }
      }).start()
    }
    override def onStop(): Unit = {
      flg = false
    }
  }

}
