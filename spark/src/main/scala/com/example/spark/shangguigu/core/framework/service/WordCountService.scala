package com.example.spark.shangguigu.core.framework.service

import com.example.spark.shangguigu.core.framework.common.TService
import com.example.spark.shangguigu.core.framework.dao.WordCountDao
import org.apache.spark.rdd.RDD

class WordCountService extends TService{

  private val wordCountDao = new WordCountDao

  //数据分析
  def DataAnalysis(): Array[(String, Int)] ={
    val lines: RDD[String] = wordCountDao.readfile("datas/1.txt")
    val words: RDD[String] = lines.flatMap(_.split(" "))
    val wordCount: RDD[(String, Int)] = words.map(word => (word, 1)).reduceByKey(_+_)
    wordCount.collect()
  }
}
