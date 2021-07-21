package com.example.spark.shangguigu.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_ByKeyFunc {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")//单核，结果只为一个part
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)), 2)

    /** wordcount
      reduceByKey:
              combineByKeyWithClassTag[V](
                (v: V) => v, //第一个值不会参与计算
                func,
                func,
                partitioner)

      aggregateByKey:
              combineByKeyWithClassTag[U](
                (v: V) => cleanedSeqOp(createZero(), v), //初始值和第一个key的value进行的分区内数据操作
                cleanedSeqOp,
                combOp,
                partitioner)

      foldByKey: 分区内、分区间处理方式一致
              combineByKeyWithClassTag[V](
                (v: V) => cleanedFunc(createZero(), v), //初始值和第一个key的value进行的分区内数据操作
                cleanedFunc,                            //分区内数据处理
                cleanedFunc,                            //分区内数据处理
                partitioner)

      combineByKey:
              combineByKeyWithClassTag(
                createCombiner, //相同 key第一条数据的处理方式
                mergeValue,     //分区内数据处理
                mergeCombiners, //分区间数据处理
                defaultPartitioner(self))
      * */
    rdd.reduceByKey(_+_).collect().foreach(println)
    rdd.aggregateByKey(0)(_+_, _+_).collect().foreach(println)
    rdd.foldByKey(0)(_+_).collect().foreach(println)
    rdd.combineByKey(v=>v, (x:Int,y:Int)=>(x+y), (x:Int,y:Int)=>(x+y)).collect().foreach(println)

    sc.stop()
  }
}
