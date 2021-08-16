package com.atguigu.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * P100 RDD的持久化
 *
 * 使用 cache() 方法来持久化数据，保存在内存中
 *
 * persist() 则选择保存在哪里。
 *
 * RDD的对象持久化操作，不一定是为了重用。在数据执行很长，或者比较重要的操作场合也可以持久化。
 */

object Spark03_RDD_Persist {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val list = List("Hello Scala", "Hello Spark")

    val rdd: RDD[String] = sc.makeRDD(list)

    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))

    val mapRDD: RDD[(String, Int)] = flatRDD.map((_, 1))

    // mapRDD.cache()
    mapRDD.persist(StorageLevel.DISK_ONLY)

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)

    println("************************")
    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()

    groupRDD.collect().foreach(println)

    sc.stop()
  }
}
