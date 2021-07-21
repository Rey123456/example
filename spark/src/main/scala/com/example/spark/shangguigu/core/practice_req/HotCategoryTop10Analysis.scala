package com.example.spark.shangguigu.core.practice_req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HotCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(sparkConf)

    /**
    Top10 热门品类
    按照每个品类的点击、下单、支付的量来统计热门品类
    综合排名 = 点击数*20%+下单数*30%+支付数*50%
    本项目需求优化为:先按照点击数排名，靠前的就排名高;如果点击数相同，再比较下单数;下单数再相同，就比较支付数。
      * */
    //读数
    val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")

    //点击
    val clickRdd: RDD[(String, Int)] = rdd.filter(line => { //先过滤
      val datas: Array[String] = line.split("_")
      datas(6)!="-1"
    }).map(line => { //取值
      val datas: Array[String] = line.split("_")
      (datas(6),1)
    }).reduceByKey(_+_) //统计

    //下单 1-2-3
    val orderRdd: RDD[(String, Int)] = rdd.filter(line => {
      val datas: Array[String] = line.split("_")
      datas(8) != "null"
    }).flatMap(line =>{
      val datas: Array[String] = line.split("_")
      datas(8).split(",").map(id => (id,1))
    }).reduceByKey(_+_)

    //支付
    val payRdd: RDD[(String, Int)] = rdd.filter(line => {
      val datas: Array[String] = line.split("_")
      datas(10) != "null"
    }).flatMap(line =>{
      val datas: Array[String] = line.split("_")
      datas(10).split(",").map(id => (id,1))
    }).reduceByKey(_+_)

    //连接 元组排序 (品类 ,(点击,下单,支付))
    //join（相同key） zip（连接与数量和位置有关） leftOuterJoin（以左表为主） cogroup（connect + group）
    val cogroupRdd: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] =
      clickRdd.cogroup(orderRdd, payRdd) //连接
    val analysisRdd: RDD[(String, (Int, Int, Int))] = cogroupRdd.mapValues {
      case (clickIter, orderIter, payIter) => {
        var cilckCnt = 0
        var orderCnt = 0
        var payCnt = 0

        val clickiterator: Iterator[Int] = clickIter.iterator
        if (clickiterator.hasNext) {
          cilckCnt = clickiterator.next()
        }
        val orderiterator: Iterator[Int] = orderIter.iterator
        if (orderiterator.hasNext)
          orderCnt = orderiterator.next()
        val payiterator: Iterator[Int] = payIter.iterator
        if (payiterator.hasNext)
          payCnt = payiterator.next()

        (cilckCnt, orderCnt, payCnt)
      }
    } //读出数据，组成元组
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
