package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * glom 的用法
 *
 * 一个分区的数据形成一个数组
 * 现在看看例子
 */

object Spark05_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - glom
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    // 【1，2】，【3，4】   分区内取最大值
    // 【2】，【4】
    // 然后求和 【6】
    val glomRDD: RDD[Array[Int]] = rdd.glom()

    val maxRDD: RDD[Int] = glomRDD.map(
      array => {
        array.max
      }
    )

    println("分区最大值之和： " + maxRDD.collect().sum)

    sc.stop()
  }
}
