package com.example.spark.shangguigu.core.framework.common
import com.example.spark.shangguigu.core.framework.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

trait TApplication {

  def start(master:String = "local[*]", app:String = "Application")(op : => Unit): Unit ={ //控制抽象
    val sparkConf: SparkConf = new SparkConf().setAppName(app).setMaster(master)
    val sc: SparkContext = new SparkContext(sparkConf)
    EnvUtil.put(sc)

    try {
      op
    }catch {
      case ex => println(ex.getMessage)
    }

    sc.stop()
    EnvUtil.clear()
  }
}
