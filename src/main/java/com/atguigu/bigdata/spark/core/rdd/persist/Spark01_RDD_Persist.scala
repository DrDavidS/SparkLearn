package com.atguigu.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * P94-P99 RDD的持久化
 *
 * 以简单的WordCount为例子
 * 你可能觉得重复使用很简单？实际上RDD不保存结果。所以实际上是重新算了一次
 *
 * 对象重新使用了，但是数据无法重用。所以真的要重用的话，我们可以采用持久化操作
 *
 */

object Spark01_RDD_Persist {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val list = List("Hello Scala", "Hello Spark")

    val rdd: RDD[String] = sc.makeRDD(list)

    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))

    val mapRDD: RDD[(String, Int)] = flatRDD.map((_, 1))

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)

    println("************************")

    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()

    groupRDD.collect().foreach(println)

    sc.stop()
  }
}
