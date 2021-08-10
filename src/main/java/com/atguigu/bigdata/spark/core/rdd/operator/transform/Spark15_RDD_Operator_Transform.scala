package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * P65 https://www.bilibili.com/video/BV11A411L7CK?p=65
 * KV类型 reduceByKey
 *
 *
 */

object Spark15_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - KV类型

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4)))

    // 我们希望相同的key放在一个组里面
    // reduceByKey： 相同的key数据进行value的数据聚合操作
    // scala中一般是两两聚合，spark基于scala开发，所以也是两两聚合
    // 【1，2，3】
    // reduceByKey中，如果key只有一个，不参与运算，直接返回
    val reduceRDD: RDD[(String, Int)] = rdd.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)

    sc.stop()
  }
}
