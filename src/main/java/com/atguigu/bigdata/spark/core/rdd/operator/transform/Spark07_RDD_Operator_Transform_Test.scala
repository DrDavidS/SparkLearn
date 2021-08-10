package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * P55 https://www.bilibili.com/video/BV11A411L7CK?p=55
 * filter 的用法
 *
 * 例子：
 * 从apache.log中获取2015年5月17日的请求路径
 */

object Spark07_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - filter
    val rdd: RDD[String] = sc.textFile("datas/apache.log")
    rdd.filter(
      (line: String) => {
        val data: Array[String] = line.split(" ")
        val time: String = data(3)
        time.startsWith("17/05/2015")
      }
    ).collect.foreach(println)

    sc.stop()
  }
}
