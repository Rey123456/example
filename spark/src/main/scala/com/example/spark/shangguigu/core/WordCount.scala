package com.example.spark.shangguigu.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("wordcount").setMaster("local")
    val sc: SparkContext = new SparkContext(sparkConf)

//    val lines: RDD[String] = sc.textFile("datas")
//    val words: RDD[String] = lines.flatMap(_.split(" "))
//    val wordCount: RDD[(String, Int)] = words.map(word => (word, 1)).reduceByKey(_+_)
//    wordCount.collect().foreach(println)
    //wordCount.foreach(println)

    wordcount11(sc)

    sc.stop()
  }

  //groupBy
  def wordcount1(sc : SparkContext) : Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("hello spark", "hello scala"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val group: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    val wordCount: RDD[(String, Int)] = group.mapValues(iter => iter.size)
  }

  //groupByKey shuffle 效率不高
  def wordcount2(sc : SparkContext) : Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("hello spark", "hello scala"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val mapOne: RDD[(String, Int)] = words.map((_,1))
    val group: RDD[(String, Iterable[Int])] = mapOne.groupByKey()
    val wordCount: RDD[(String, Int)] = group.mapValues(iter => iter.size)
  }

  //reduceByKey
  def wordcount3(sc : SparkContext) : Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("hello spark", "hello scala"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val mapOne: RDD[(String, Int)] = words.map((_,1))
    val wordCount: RDD[(String, Int)] = mapOne.reduceByKey(_+_)
  }

  //aggregateByKey
  def wordcount4(sc : SparkContext) : Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("hello spark", "hello scala"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val mapOne: RDD[(String, Int)] = words.map((_,1))
    val wordCount: RDD[(String, Int)] = mapOne.aggregateByKey(0)(_+_,_+_)
  }

  //foldByKey
  def wordcount5(sc : SparkContext) : Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("hello spark", "hello scala"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val mapOne: RDD[(String, Int)] = words.map((_,1))
    val wordCount: RDD[(String, Int)] = mapOne.foldByKey(0)(_+_)
  }

  //combineByKey
  def wordcount6(sc : SparkContext) : Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("hello spark", "hello scala"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val mapOne: RDD[(String, Int)] = words.map((_,1))
    val wordCount: RDD[(String, Int)] = mapOne.combineByKey(
      v => v,
      (x:Int, y) => x + y,
      (x:Int, y:Int) => x + y,
    )
  }

  //countByKey
  def wordcount7(sc : SparkContext) : Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("hello spark", "hello scala"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val mapOne: RDD[(String, Int)] = words.map((_,1))
    val wordCount: collection.Map[String, Long] = mapOne.countByKey()
  }

  //countByValues
  def wordcount8(sc : SparkContext) : Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("hello spark", "hello scala"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordCount: collection.Map[String, Long] = words.countByValue()
  }

  //reduce
  def wordcount9(sc : SparkContext) : Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("hello spark", "hello scala"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val mapWord: RDD[mutable.Map[String, Long]] =
      words.map(word => mutable.Map[String, Long]((word, 1)))
    val wordCount: mutable.Map[String, Long] = mapWord.reduce((map1, map2) => {
      map2.foreach({
        case (word, count) => {
          val newCount = map1.getOrElse(word, 0l) + count
          map1.put(word, newCount)
        }
      })
      map1
    })
    println(wordCount)
  }

  //aggregate
  def wordcount10(sc : SparkContext) : Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("hello spark", "hello scala"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val mapWord: RDD[mutable.Map[String, Long]] =
      words.map(word => mutable.Map[String, Long]((word, 1)))
    val wordCount: mutable.Map[String, Long] =
      mapWord.aggregate(mutable.Map.empty[String, Long])(
      (map1, map2) => {
        map2.foreach({
          case (word, count) => {
            val newCount = map1.getOrElse(word, 0l) + count
            map1.put(word, newCount)
          }
        })
        map1
      },
      (map1, map2) => {
        map2.foreach({
          case (word, count) => {
            val newCount = map1.getOrElse(word, 0l) + count
            map1.put(word, newCount)
          }
        })
        map1
      }
    )
    println(wordCount)
  }

  //fold
  def wordcount11(sc : SparkContext) : Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("hello spark", "hello scala"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val mapWord: RDD[mutable.Map[String, Long]] =
      words.map(word => mutable.Map[String, Long]((word, 1)))
    val wordCount: mutable.Map[String, Long] =
      mapWord.fold(mutable.Map.empty[String, Long])(
        (map1, map2) => {
          map2.foreach({
            case (word, count) => {
              val newCount = map1.getOrElse(word, 0l) + count
              map1.put(word, newCount)
            }
          })
          map1
        }
      )
    println(wordCount)
  }

}
