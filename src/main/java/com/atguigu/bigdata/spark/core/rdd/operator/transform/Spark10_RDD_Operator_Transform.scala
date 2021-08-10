package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * P58 https://www.bilibili.com/video/BV11A411L7CK?p=58
 * coalesce 的用法，缩减分区
 * 默认情况下它不会打乱重新组合，仅仅是缩减分区而已
 */

object Spark10_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - coalesce
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)

    // 可能会导致数据不均衡，数据倾斜
    // 如果要数据均衡，可以先 shuffle = true

    val newRDD: RDD[Int] = rdd.coalesce(2, shuffle = true) // 缩减分区

    newRDD.saveAsTextFile("output")

    sc.stop()
  }
}
