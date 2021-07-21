package com.example.spark.shangguigu.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Create {

  def main(args: Array[String]): Unit = {
    /** [*]当前系统的最大可用核数，不写为单核
      * */
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    /**从内存中创建RDD，将内存中集合的数据作为处理的数据源*/
    val seq = Seq[Int](1,2,3,4)
    val rdd: RDD[Int] = sc.parallelize(seq)
    rdd.collect().foreach(println)
    val rdd1: RDD[Int] = sc.makeRDD(seq)//内部调用parallelize
    rdd1.collect().foreach(println)

    /**从文件中创建RDD，将文件中的数据作为处理的数据源*/
    val rdd2: RDD[String] = sc.textFile("datas/1.txt")//文件路径、目录、通配符 以行为单位读取数据
    rdd2.collect().foreach(println)
    val rdd3: RDD[(String, String)] = sc.wholeTextFiles("datas")//以文件为单位读取数据 结果为元组，第一个元素表示文件路径，第二个元素表示文件内容
    rdd3.collect().foreach(println)


    sc.stop()
  }
}
