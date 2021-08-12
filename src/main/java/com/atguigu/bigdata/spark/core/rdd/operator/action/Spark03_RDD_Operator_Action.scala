package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * P83 行动算子
 *
 * aggregate 和 aggregateByKey的区别：
 *
 * aggregateByKey 初始值只参与分区内计算
 *
 * aggregate 还会参与分区间计算
 *
 */

object Spark03_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    // 环境准备
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)



    // TODO 行动算子
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2) // 两个分区

    // val result: Int = rdd.aggregate(0)(_ + _, _ + _)
    val result: Int = rdd.fold(10)(_ + _)  // 一回事

    println(result)

    sc.stop()
  }
}
