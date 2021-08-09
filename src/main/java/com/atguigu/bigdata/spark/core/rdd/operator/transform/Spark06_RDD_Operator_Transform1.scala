package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * P52 https://www.bilibili.com/video/BV11A411L7CK?p=52
 * groupBy 的用法
 *
 * 按首字母来分组，现在看看例子
 */

object Spark06_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - groupBy
    val rdd: RDD[String] = sc.makeRDD(List("Hello", "Scala", "Hello", "Spark"), 2)

    // 分组和分区没有必然关系
    val groupRDD: RDD[(Char, Iterable[String])] = rdd.groupBy(_.charAt(0))

    groupRDD.collect().foreach(println)
    sc.stop()
  }
}
