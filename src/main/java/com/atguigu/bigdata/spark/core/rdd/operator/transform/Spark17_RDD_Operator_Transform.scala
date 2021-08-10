package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * P68 https://www.bilibili.com/video/BV11A411L7CK?p=68
 * KV类型
 *
 * aggregateByKey
 *
 */

object Spark17_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - KV类型 aggregateByKey

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)), 2)
    // (a分区，【1,2】)，(a分区，【3,4】)
    // (a,2),(a,4)
    // (a,6)

    // aggregateByKey 存在函数柯里化，有两个参数列表
    // 第一个参数列表：初始值
    //   主要用于当碰见第一个key的时候，和value做分区内计算
    // 第二个参数列表有传递两个参数
    //   第一个参数表示分区内计算规则
    //   第二个参数表示分区间计算规则

    //    rdd.aggregateByKey(0)(
    //      (x, y) => math.max(x, y),
    //      (x, y) => x + y
    //    ).collect().foreach(println)

    rdd.aggregateByKey(0)(
      math.max, _ + _
    ).collect().foreach(println)

    sc.stop()
  }
}
