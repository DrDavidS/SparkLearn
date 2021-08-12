package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * P75 https://www.bilibili.com/video/BV11A411L7CK?p=75
 * KV类型
 * Join 的操作
 * 两个不同数据源的数据，相同key的value会链接再一起，形成元组
 * 如果两个数据源中key没有匹配上，数据不会出现在结果中
 * 如果两个数据源中key有多个相同的，会依次匹配，可能会出现笛卡尔乘积，数据量会几何增长，可能导致性能降低
 */

object Spark21_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - KV类型 Join

    val rdd1: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("a", 1), ("b", 2), ("c", 3), ("f", 8)
      ))

    val rdd2: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("a", 4), ("b", 5), ("c", 6), ("e", 9)
      ))

    val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

    joinRDD.collect().foreach(println)

    sc.stop()
  }
}

