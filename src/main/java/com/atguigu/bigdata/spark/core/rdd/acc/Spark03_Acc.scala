package com.atguigu.bigdata.spark.core.rdd.acc

/**
 * P106
 *
 * 就改了一个方法名称，为什么变化了？
 * 累加器可能出现少加或者多加的情况。
 *
 */

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Acc {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    // 获取系统累加器
    // Spark默认提供了简单数据聚合的累加器
    val sumAcc: LongAccumulator = sc.longAccumulator("sum")

    val mapRDD: RDD[Int] = rdd.map(
      (num: Int) => {
        // 使用累加器
        sumAcc.add(num)
        num
      }
    )

    // 获取累加器的值
    // 少加：转换算子中调用累加器，如果没有行动算子，那么不会执行
    // 多加：累加器是全局共享的，如果你写两个collect，就会执行两次，答案是20
    mapRDD.collect()
    println(sumAcc.value)

    sc.stop()
  }
}
