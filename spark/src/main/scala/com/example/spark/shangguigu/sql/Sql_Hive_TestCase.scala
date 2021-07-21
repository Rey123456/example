package com.example.spark.shangguigu.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Sql_Hive_TestCase {

  def main(args: Array[String]): Unit = {
    //创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sql").set("spark.sql.legacy.createHiveTableByDefault","false")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate() //.enableHiveSupport()

    println(spark.conf.get("spark.sql.legacy.createHiveTableByDefault"))
    /** 使用内置hive
      * 1 张用户行为表，1 张城市表，1 张产品表；求各区域热门商品 **Top3**
      * */

    /**
      *  数据准备
      * */
    spark.sql("show tables").show()
    spark.sql("create database testcase")
    spark.sql("show databases").show()
    spark.sql("use testcase")

    spark.sql(
      """
        |CREATE TABLE user_visit_action(
        |date string,
        |user_id bigint,
        |session_id string,
        |page_id bigint,
        |action_time string,
        |search_keyword string,
        |click_category_id bigint,
        |click_product_id bigint,
        |order_category_ids string,
        |order_product_ids string,
        |pay_category_ids string,
        |pay_product_ids string,
        |city_id bigint)
        |using csv
        |options(
        |path '/Users/wenjingdong/IdeaProjects/github/newTec/datas/user_visit_action.txt',
        |sep '\t')
      """.stripMargin)
    spark.sql("show tables").show()

    spark.sql(
      """
        |CREATE TABLE product_info(
        |product_id bigint,
        |product_name string,
        |extend_info string)
        |using csv
        |options(
        |path '/Users/wenjingdong/IdeaProjects/github/newTec/datas/product_info.txt',
        |sep '\t')
      """.stripMargin)
    spark.sql("show tables").show()

    spark.sql(
      """
        |CREATE TABLE city_info(
        |city_id bigint,
        |city_name string,
        |area string)
        |using csv
        |options(
        |path '/Users/wenjingdong/IdeaProjects/github/newTec/datas/city_info.txt',
        |sep '\t')
      """.stripMargin)
    spark.sql("show tables").show()

//    spark.sql("select * from user_visit_action limit 10").show()

    /**
      * 这里的热门商品是从点击量的维度来看的，计算各个区域前三大热门商品，并备注上每 个商品在主要城市中的分布比例
      * 1.
      *
      * */

    //查询基本数据
    spark.sql(
      """
        |select
        |b.product_name
        |,c.area
        |,c.city_name
        |from user_visit_action as a
        |join product_info as b on a.click_product_id=b.product_id
        |join city_info as c on a.city_id = c.city_id
        |where a.click_product_id > -1
      """.stripMargin).createOrReplaceTempView("t1")

    // 根据区域，商品进行数据聚合
    spark.udf.register("cityRemark", functions.udaf(new CityRemarkUDAF))
    spark.sql(
      """
        |select
        |area
        |,product_name
        |,count(1) as clickCnt
        |,cityRemark(city_name) as remark
        |from t1
        |group by area,product_name
      """.stripMargin).createOrReplaceTempView("t2")

    //区域内对点击数量进行排行
    spark.sql(
      """
        |select *
        |,rank() over (partition by area order by clickCnt desc) as rank
        |from t2
      """.stripMargin).createOrReplaceTempView("t3")

    // 取前3名
    spark.sql(
      """
        | select
        |     *
        | from t3 where rank <= 3
      """.stripMargin).show(false)

    //关闭环境
    spark.close()
  }

  /**
    *自定义聚合函数：实现城市备注功能
    * 1 继承org.apache.spark.sql.expressions.Aggregator，定义范型
    *   IN：输入数据类型 String
    *   BUF：缓冲区数据类型 (Count, Map[(city,cnt),(city,cnt)])
    *   OUT：输出数据类型 String
    * 2 重写方法
    * */
  case class Buffer(var total:Long, var cityMap: mutable.Map[String, Long])
  class CityRemarkUDAF extends Aggregator[String, Buffer, String]{
    // 缓冲区初始化
    override def zero: Buffer = {
      Buffer(0, mutable.Map.empty[String, Long])
    }

    // 更新缓冲区数据
    override def reduce(buff: Buffer, city: String): Buffer = {
      buff.total += 1
      var newCnt = buff.cityMap.getOrElse(city, 0l) + 1
      buff.cityMap.update(city, newCnt)
      buff
    }

    // 合并缓冲区数据
    override def merge(buff1: Buffer, buff2: Buffer): Buffer = {
      buff1.total += buff2.total

      val map1 = buff1.cityMap
      val map2 = buff2.cityMap

//      map2.foreach{
//        case (city, cnt) => {
//          val newCnt = map1.getOrElse(city, 0l) + cnt
//          map1.update(city, newCnt)
//        }
//      }
//      buff1.cityMap = map1

      buff1.cityMap = map1.foldLeft(map2) {
        case ( map, (city, cnt) ) => {
          val newCnt = map.getOrElse(city, 0l) + cnt
          map.update(city, newCnt)
          map
        }
      }

      buff1
    }

    // 将统计的结果生成字符串信息
    override def finish(buff: Buffer): String = {
      val remarkList = ListBuffer[String]()

      val totalCnt = buff.total
      val cityMap = buff.cityMap

      //降序排列
      val cityCntList = cityMap.toList.sortWith(
        (left, right) => {
          left._2 > right._2
        }
      ).take(2)

      var resume = 0.0
      cityCntList.foreach{
        case (city,  cnt) => {
          val r = cnt * 100.0 / totalCnt
          remarkList.append(s"${city} ${r.formatted("%.2f")}%")
          resume += r
        }
      }
      if(resume < 100){
        remarkList.append(s"其他 ${(100.0-resume).formatted("%.2f")}%")
      }

      remarkList.mkString(",")
    }

    override def bufferEncoder: Encoder[Buffer] = Encoders.product
    override def outputEncoder: Encoder[String] = Encoders.STRING
  }
}
