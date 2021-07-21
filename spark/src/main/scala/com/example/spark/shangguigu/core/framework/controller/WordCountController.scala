package com.example.spark.shangguigu.core.framework.controller

import com.example.spark.shangguigu.core.framework.common.TController
import com.example.spark.shangguigu.core.framework.service.WordCountService

class WordCountController extends TController{

  private val wordCountService = new WordCountService

  //调度
  def dispatch()={
    val array: Array[(String, Int)] = wordCountService.DataAnalysis()
    array.foreach(println)
  }
}
