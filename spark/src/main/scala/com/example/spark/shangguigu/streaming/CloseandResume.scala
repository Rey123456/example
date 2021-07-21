package com.example.spark.shangguigu.streaming

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

object CloseandResume {

  // 如果想要关闭采集器，那么需要创建新的线程
  // 而且需要在第三方程序中增加关闭状态
  class MonitorStop(ssc: StreamingContext) extends Runnable {
    override def run(): Unit = {
      val fs: FileSystem = FileSystem.get(ssc.sparkContext.hadoopConfiguration)

      while(true){
        Thread.sleep(5000)
        val state: StreamingContextState = ssc.getState()
        // 计算节点不在接收新的数据，而是将现有的数据处理完毕，然后关闭
        // Mysql : Table(stopSpark) => Row => data
        // Redis : Data（K-V）
        // ZK    : /stopSpark
        // HDFS  : /stopSpark
        val bool: Boolean = fs.exists(new Path("path"))//通过第三方路径
        if(bool){
          if(state == StreamingContextState.ACTIVE){
            ssc.stop(true, true)
            System.exit(0) //
          }
        }
      }
    }
  }

  def createSSc() : StreamingContext = {

    val update = (seq: Seq[Int], buff: Option[Int]) => {
      //当前批次内容的计算
      val sum: Int = seq.sum
      //取出状态信息中上一次状态, 第一次为0，所以用Option类型
      val lastStatu = buff.getOrElse(0)
      Option(sum + lastStatu)
    }

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Stream")
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("ck")

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val wordCount: DStream[(String, Int)] =
      lines.flatMap(_.split(" ")).map((_,1)).updateStateByKey(update)

    wordCount.print()
    ssc
  }

  def main(args: Array[String]): Unit = {

    val ssc: StreamingContext =
      StreamingContext.getActiveOrCreate("ck", () => createSSc())

    new Thread(new MonitorStop(ssc)).start()

    ssc.start()
    ssc.awaitTermination()// block 阻塞main线程
  }
}
