package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * P60 https://www.bilibili.com/video/BV11A411L7CK?p=60
 * sortBy
 */

object Spark12_RDD_Operator_Transform_1 {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - sortBy
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)

    val sortRDD: RDD[Int] = rdd.sortBy((num: Int) => num)  // 默认是有个shuffle的概念的Spark12_RDD_Operator_Transform_sortBy$

    sortRDD.saveAsTextFile("output")

    sc.stop()
  }
}
