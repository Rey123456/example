package com.example.spark.shangguigu.core.practice_req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Req2_HotCategoryTop10SessionAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(sparkConf)

    /**
    Top10 热门品类中每个品类的 Top10 活跃 Session 统计
      * */
    //读数
    val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    rdd.cache()

    val top10Rdd:Array[String] = getTop10(rdd)

    //1. 过滤非必要数据
    val filterRdd: RDD[String] = rdd.filter(action => {
      val datas: Array[String] = action.split("_")
      datas(6) != "-1" && top10Rdd.contains(datas(6))
    })

    //2. 统计 ((品类，session)，count) 点击量统计
    val reduceRdd: RDD[((String, String), Int)] = filterRdd.map(action => {
      val datas: Array[String] = action.split("_")
      ((datas(6),datas(2)),1)
    }).reduceByKey(_+_)

    //格式转换
    val mapRdd: RDD[(String, (String, Int))] = reduceRdd.map{
      case ((pid,sid),cnt) => (pid,(sid,cnt))
    }

    //分组 sort
    val groupRdd: RDD[(String, Iterable[(String, Int)])] = mapRdd.groupByKey()
    val resultRdd: RDD[(String, List[(String, Int)])] = groupRdd.mapValues(iter => {
      iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
    })

    //结果打印
    resultRdd.collect().foreach(println)

    sc.stop()
  }

  def getTop10(rdd:RDD[String]): Array[String] ={

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

    analysisRdd.sortBy(_._2, false).take(10).map(_._1) //排序
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
