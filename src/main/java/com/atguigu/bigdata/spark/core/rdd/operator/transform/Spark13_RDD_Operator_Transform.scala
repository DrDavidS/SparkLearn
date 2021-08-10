package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * P61 https://www.bilibili.com/video/BV11A411L7CK?p=61
 * 交集差集并集
 * 交集差集并集操作要求数据源类型是一致的
 *
 * 但是zip可以不一样
 *
 */

object Spark13_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - 双value

    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val rdd2: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6))
    val rdd7: RDD[String] = sc.makeRDD(List("3", "4", "5", "6"))

    // 交集
    val rdd3: RDD[Int] = rdd1.intersection(rdd2)
    // rdd1.intersection(rdd7) 类型不匹配
    println("交集： " + rdd3.collect().mkString(","))

    // 并集
    val rdd4: RDD[Int] = rdd1.union(rdd2)
    println("并集： " + rdd4.collect().mkString(","))

    // 差集
    val rdd5: RDD[Int] = rdd1.subtract(rdd2)
    println("差集： " + rdd5.collect().mkString(","))

    // 拉链
    // 数据类型可以不一致
    val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)
    val rdd8: RDD[(Int, String)] = rdd1.zip(rdd7)
    println("拉链1： " + rdd6.collect().mkString(","))
    println("拉链2： " + rdd8.collect().mkString(","))

    sc.stop()
  }
}
