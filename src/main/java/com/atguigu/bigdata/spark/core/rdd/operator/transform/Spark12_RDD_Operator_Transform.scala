package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * P59 https://www.bilibili.com/video/BV11A411L7CK?p=59
 * coalesce 的用法，扩大分区
 * 实际上扩大分区还有其他方法 repartition
 */

object Spark12_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - coalesce
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)

    // 可以扩大分区，但是不进行shuffle操作就没有意义
    // 记得 shuffle = true
    // val newRDD: RDD[Int] = rdd.coalesce(3, shuffle = true) // 扩大分区
    val newRDD: RDD[Int] = rdd.repartition(3) // 底层代码就是 coalesce
    newRDD.saveAsTextFile("output")

    sc.stop()
  }
}
