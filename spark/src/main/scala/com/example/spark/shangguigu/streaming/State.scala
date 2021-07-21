package com.example.spark.shangguigu.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object State {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("cp")

    // 无状态数据操作，只对当前的采集周期内的数据进行处理
    // 在某些场合下，需要保留数据统计结果（状态），实现数据的汇总
    // 使用有状态操作时，需要设定检查点路径
    val stream = ssc.socketTextStream("localhost", 9999)

    // updateStateByKey：根据key对数据的状态进行更新
    // 传递的参数中含有两个值
    // 第一个值表示相同的key的value数据
    // 第二个值表示缓存区相同key的value数据
    val state: DStream[(String, Int)] = stream.map((_, 1)).updateStateByKey {
      (seq: Seq[Int], buff: Option[Int]) => {
        val count = buff.getOrElse(0) + seq.sum
        Option(count)
      }
    }
    state.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
