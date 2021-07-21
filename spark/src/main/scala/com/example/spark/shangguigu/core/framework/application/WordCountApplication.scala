package com.example.spark.shangguigu.core.framework.application

import com.example.spark.shangguigu.core.framework.common.TApplication
import com.example.spark.shangguigu.core.framework.controller.WordCountController

object WordCountApplication extends App with TApplication{

  //启动应用程序
  start(){
    val controller: WordCountController = new WordCountController
    controller.dispatch()
  }
}
