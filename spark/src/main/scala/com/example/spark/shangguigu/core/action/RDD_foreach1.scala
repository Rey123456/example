package com.example.spark.shangguigu.core.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_foreach1 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("action")
    val sc = new SparkContext(sparkConf)

    //val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)
    val rdd: RDD[Int] = sc.makeRDD(List[Int]())

    val user: User = new User()

    rdd.foreach(
      num => {
        println("age = " + (user.age + num))
      }
    )

    sc.stop()
  }

  /** Task not serializable(org.apache.spark.util.ClosureCleaner$.ensureSerializable)
    * object not serializable (class: com.practice.spark.core.action.RDD_foreach1$User, value: com.practice.spark.core.action.RDD_foreach1$User@49798e84)
    * */
  //class User extends Serializable {
  /**样例类在编译时，会自动混入序列化特质(实现可序列化接口)*/
  //case class User(){
  /**当没有数据时依旧会报错
    * RDD算子中传递的函数是会包含闭包（把外部变量引入到函数内部形成闭合效果，改变变量的生命周期）操作，就会进行检测功能
    * 闭包检测
    * */
  class User{
    val age :Int = 30
  }
}
