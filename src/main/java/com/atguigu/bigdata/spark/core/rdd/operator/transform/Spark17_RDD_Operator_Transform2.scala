package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * P70 https://www.bilibili.com/video/BV11A411L7CK?p=70
 * KV类型
 *
 * foldByKey
 *
 */

object Spark17_RDD_Operator_Transform2 {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - KV类型 aggregateByKey

    val rdd: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("a", 1), ("a", 2), ("b", 3),
        ("b", 4), ("b", 5), ("a", 6)
      ), 2)

    //    rdd.aggregateByKey(0)(
    //      _ + _, _ + _
    //    ).collect().foreach(println)

    // 如果分区内和分区间计算规则相同，可以用foldByKey
    rdd.foldByKey(0)(_ + _).collect().foreach(println)

    // 效果很reduceByKey一样

    sc.stop()
  }
}
