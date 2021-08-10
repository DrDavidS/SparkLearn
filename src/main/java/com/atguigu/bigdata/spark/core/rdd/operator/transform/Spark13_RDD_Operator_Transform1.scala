package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * P61 https://www.bilibili.com/video/BV11A411L7CK?p=61
 *
 * 分区拉链
 * 注意，拉链需要分区和元素都一致
 *
 */

object Spark13_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - 双value
    // Can't zip RDDs with unequal numbers of partitions: List(2, 4)
    // 分区数据需要保持一致
    // 元素的数据量也要一致
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val rdd2: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6), 2)

    // 拉链
    // 数据类型可以不一致
    val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)

    println("拉链1： " + rdd6.collect().mkString(","))

    sc.stop()
  }
}
