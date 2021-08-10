package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * P60 https://www.bilibili.com/video/BV11A411L7CK?p=60
 * sortBy
 */

object Spark12_RDD_Operator_Transform_sortBy {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - sortBy
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("1", 1), ("11", 2), ("2", 3)), 2)

    // 字符顺序排列，默认升序
    val newRDD: RDD[(String, Int)] = rdd.sortBy((t: (String, Int)) => t._1, false)

    newRDD.collect().foreach(println)

    sc.stop()
  }
}
