package com.example.spark.shangguigu.core.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Persist {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("persist")//单核，结果只为一个part
    val sc = new SparkContext(sparkConf)
    sc.setCheckpointDir("cp")

    val list: List[String] = List("hello spark", "hello scala")
    val rdd: RDD[String] = sc.makeRDD(list)
    val flatRdd: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRdd: RDD[(String, Int)] = flatRdd.map(word => {
      println("@@@@@@@@@@@@@@@")
      (word, 1)
    })


    /** cache
    将数据临时存储在内存中进行数据重用
    会在血缘关系中添加新的依赖，一旦出现问题，可以重头读取数据
    作业执行完毕临时数据丢失
      * */
    mapRdd.cache()
    /**persist
    内存、磁盘、重复数的各种拼凑
    将数据临时存储在磁盘、内存中进行数据重用：涉及到磁盘IO，性能较低，但是数据安全
    作业执行完毕临时数据丢失
      * */
    mapRdd.persist(StorageLevel.DISK_ONLY)
    /**checkpoint
    需要落盘，指定检查点保存路径 sc.setCheckpointDir("cp")：涉及到磁盘IO，性能较低，但是数据安全
    保存的文件当作业执行完毕后，不会被删除
    一般保存路径都是在分布式存储系统
    为保证数据安全一般会独立执行作业（代码流程中）：为了提高效率可以和cache一起使用
    执行过程中，会切断血缘关系，重新建立新的血缘关系，等同于改变了数据源
      * */
    mapRdd.checkpoint()

    println(mapRdd.toDebugString)
    //mapRdd的重用
    val reduceRdd: RDD[(String, Int)] = mapRdd.reduceByKey(_+_)
    reduceRdd.collect().foreach(println)
    println(mapRdd.toDebugString)
    println("8********************************")
    val groupRdd: RDD[(String, Iterable[Int])] = mapRdd.groupByKey()
    groupRdd.collect().foreach(println)

    sc.stop()

  }

}
