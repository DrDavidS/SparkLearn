package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * P77 https://www.bilibili.com/video/BV11A411L7CK?p=77
 * KV类型
 * cogroup 的操作
 *
 * connect + group
 *
 * 分组 + 链接
 *
 */

object Spark23_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - KV类型 cogroup

    val rdd1: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("a", 1), ("b", 2), ("c", 3)
      ))

    val rdd2: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("a", 4), ("b", 5), ("c", 6), ("c", 7)
      ))


    val cgRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)

    cgRDD.collect().foreach(println)

    sc.stop()
  }
}

