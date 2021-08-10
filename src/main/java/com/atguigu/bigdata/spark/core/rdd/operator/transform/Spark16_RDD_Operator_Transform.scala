package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * P66 https://www.bilibili.com/video/BV11A411L7CK?p=66
 * KV类型 groupByKey
 *
 * groupByKey 会导致数据打乱重组，存在shuffle操作
 * 在Spark中，shuffle操作必须落盘，不能再内存中等待，否则会内存溢出
 * shuffle操作的性能很低，因为存在磁盘IO
 *
 * 再优化：在每个分区提前聚合一部分，再落盘。这样IO少了，性能提升了
 *
 * reduceByKey 支持分区内预先聚合，有效减少shuffle落盘的数据量，性能比较高
 *
 * 所以分组聚合的情况下推荐reduceByKey，只分组不聚合，就用groupByKey
 *
 */

object Spark16_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - KV类型 groupByKey

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4)))

    // groupByKey 将数据源中的相同key的数据分在一个组中
    // 形成一个对偶元组，
    // 元组中的第一个元素就是key，第二个就是对应value的集合
    val groupRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    groupRDD.collect().foreach(println)

    val groupRDD1: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)
    groupRDD1.collect().foreach(println)

    sc.stop()
  }
}
