package com.example.spark.shangguigu.core.practice_req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object HotCategoryTop10Analysis2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(sparkConf)

    /**
    Top10 热门品类
    按照每个品类的点击、下单、支付的量来统计热门品类
    综合排名 = 点击数*20%+下单数*30%+支付数*50%
    本项目需求优化为:先按照点击数排名，靠前的就排名高;如果点击数相同，再比较下单数;下单数再相同，就比较支付数。

    Q:存在大量shuffle操作（reduceByKey）
    reducebykey 聚合算子，spark会提供优化，缓存

    优化后还是有shuffle，可以尝试使用累加器进行操作
      * */
    //读数
    val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")

    val flatRdd: RDD[(String, (Int, Int, Int))] = rdd.flatMap(action => {
      val datas: Array[String] = action.split("_")
      if (datas(6) != "-1") {
        //点击
        List((datas(6), (1, 0, 0)))
      } else if (datas(8) != "null") {
        //下单
        datas(8).split(",").map(id => (id, (0, 1, 0)))
      } else if (datas(10) != "null") {
        //支付
        datas(10).split(",").map(id => (id, (0, 0, 1)))
      } else {
        Nil
      }
    })
    val analysisRdd: RDD[(String, (Int, Int, Int))] = flatRdd.reduceByKey((t1, t2) => (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3))

    val resultRdd: Array[(String, (Int, Int, Int))] =
      analysisRdd.sortBy(_._2, false).take(10) //排序

    //结果打印
    resultRdd.foreach(println)

    sc.stop()
  }

  case class UserVisitAction(
                              date: String,//用户点击行为的日期
                              user_id: Long,//用户的 ID
                              session_id: String,//Session 的 ID
                              page_id: Long,//某个页面的 ID
                              action_time: String,//动作的时间点
                              search_keyword: String,//用户搜索的关键词
                              click_category_id: Long,//某一个商品品类的 ID 6
                              click_product_id: Long,//某一个商品的 ID
                              order_category_ids: String,//一次订单中所有品类的ID集合 8
                              order_product_ids: String,//一次订单中所有商品的ID集合
                              pay_category_ids: String,//一次支付中所有品类的 ID 集合 10
                              pay_product_ids: String,//一次支付中所有商品的 ID 集合
                              city_id: Long
                            )
}
