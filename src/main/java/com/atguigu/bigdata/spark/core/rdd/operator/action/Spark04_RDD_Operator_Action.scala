package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * P84 行动算子
 *
 * countByValue  countByKey
 * 统计出现次数
 *
 */

object Spark04_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    // 环境准备
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 行动算子
    // val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2) // 两个分区，没有key
    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3))
    )

    val stringToLong: collection.Map[String, Long] = rdd.countByKey()  // 统计次数

    println(stringToLong)

    sc.stop()
  }
}
