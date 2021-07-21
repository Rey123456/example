package com.example.spark.shangguigu.core.practice_req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Req3_PageflowAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(sparkConf)

    /**
    页面单跳转化率
    user_id,action_time,page_id
      * */
    //读数
    val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")

    val dataRdd: RDD[UserVisitAction] = rdd.map(line => {
      val datas: Array[String] = line.split("_")
      new UserVisitAction(
        datas(0),
        datas(1).toLong,
        datas(2),
        datas(3).toLong,
        datas(4),
        datas(5),
        datas(6).toLong,
        datas(7).toLong,
        datas(8),
        datas(9),
        datas(10),
        datas(11),
        datas(12).toLong
      )
    })
    dataRdd.cache()

    /** 对指定页面统计跳转 1-2 2-3 3-4 4-5 5-6 6-7
      * 分母部分可以直接filter，可以只保存1-6,7并不会作为分母
      * 分子需要在形成映射后再进行filter，不然会出现映射错误
      * 1 2 6 8 3 -》1-2 2-6 6-8 8-3 ,提前删除后会形成1 2 6 3 -》1-2 2-6 6-3(错误跳转)
      * */
    val ids: List[Long] = List[Long](1,2,3,4,5,6,7)
    val okflowids: List[(Long, Long)] = ids.zip(ids.tail)

    /** 计算分母*/
    val pageIdCount: Map[Long, Long] =
      dataRdd.filter(action => ids.init.contains(action.page_id)) //init 除去最后一个的数组
        .map(action => (action.page_id,1l)).reduceByKey(_+_).collect().toMap

    /**计算分子*/
    val sessionRdd: RDD[(String, Iterable[UserVisitAction])] =
      dataRdd.groupBy(_.session_id)

    val mvRdd: RDD[(String, List[((Long, Long), Int)])] = sessionRdd.mapValues { 
      iter => {
        val sortList: List[UserVisitAction] = iter.toList.sortBy(_.action_time)
        val flowIds = sortList.map(_.page_id)
  
        /** [1,2,3,4]
              * [1,2] [2,3] [3,4]
              * Sliding 滑窗
              * [1 2 3 4]
              * [2 3 4]
              * zip 拉链
              * */
        val pageflowIds: List[(Long, Long)] = flowIds.zip(flowIds.tail)
        pageflowIds
          .filter(it => okflowids.contains(it))//将不合法的页面跳转过滤
          .map(t => (t, 1))
      }
    }
    val flatRdd: RDD[((Long, Long), Int)] = mvRdd.map(_._2).flatMap(list => list)
    val pageflowRdd: RDD[((Long, Long), Int)] = flatRdd.reduceByKey(_+_)

    /**计算单跳转换率*/
    pageflowRdd.foreach{
      case ((page1,page2),sum) => {
        val page1sum: Long = pageIdCount.getOrElse(page1, 0l)
        println(s"页面${page1}跳转到页面${page2}的单跳转换率为：" + sum.toDouble/page1sum)
      }
    }

    sc.stop()
  }

  case class UserVisitAction(
    date: String,//用户点击行为的日期
    user_id: Long,//用户的 ID 1
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
