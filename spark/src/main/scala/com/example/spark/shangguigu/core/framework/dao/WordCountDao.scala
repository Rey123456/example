package com.example.spark.shangguigu.core.framework.dao
import com.example.spark.shangguigu.core.framework.common.TDao
import com.example.spark.shangguigu.core.framework.util.EnvUtil
import org.apache.spark.rdd.RDD

class WordCountDao extends TDao{
  def readfile(path:String): RDD[String] ={
    EnvUtil.take().textFile(path)
  }
}
