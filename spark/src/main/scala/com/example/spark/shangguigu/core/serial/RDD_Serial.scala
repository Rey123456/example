package com.example.spark.shangguigu.core.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Serial {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] =
      sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))

    /**针对 task not serializable的解决办法有2
      * 1. 添加序列化
      * 2. 让属性与类剥离
      * */
    //方法1
    val search: Search = new Search("h")
    search.getMatch1(rdd).collect().foreach(println)
    search.getMatch2(rdd).collect().foreach(println)

    //方法2
    val search1: Search1 = new Search1("h")
    search1.getMatch2(rdd).collect().foreach(println)

    sc.stop()
  }


  /** 类的构造参数是类的属性，从反编译的代码中可以看出
    * query是私有属性，调用this.query
    * 构造参数需要闭包检测，就等同于类进行闭包检测
    * */
 // class Search(query:String) extends Serializable {
    case class Search(query:String) {
    def isMatch(s: String): Boolean = {
      s.contains(query)
    }
    // 函数序列化案例
    def getMatch1 (rdd: RDD[String]): RDD[String] = {
      //rdd.filter(this.isMatch)
      rdd.filter(isMatch)
    }

    // 属性序列化案例
    def getMatch2(rdd: RDD[String]): RDD[String] = {
      //rdd.filter(x => x.contains(this.query))
      rdd.filter(x => x.contains(query))
    }
  }

  class Search1(query:String) {

    // 属性序列化案例
    def getMatch2(rdd: RDD[String]): RDD[String] = {
      val q : String = query //driver端运行
      /**rdd的算子，executor端运行，引用字符串q，本身有序列化
        * s是方法的局部变量，改变生命周期形成闭包，不需要Search1类
      */
      rdd.filter(x => x.contains(q))
    }
  }
}
