package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * P76 https://www.bilibili.com/video/BV11A411L7CK?p=76
 * KV类型
 * leftJoin rightJoin 的操作
 */

object Spark22_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - KV类型 Join

    val rdd1: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("a", 1), ("b", 2), ("c", 3)
      ))

    val rdd2: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("a", 4), ("b", 5), ("c", 6)
      ))

    val rightJoinRDD: RDD[(String, (Option[Int], Int))] = rdd1.rightOuterJoin(rdd2)
    // val leftJoinRDD: RDD[(String, (Int, Option[Int]))] = rdd1.leftOuterJoin(rdd2)


    rightJoinRDD.collect().foreach(println)

    sc.stop()
  }
}

