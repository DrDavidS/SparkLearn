package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * glom 的用法
 *
 * 一个分区的数据形成一个数组
 *
 */

object Spark05_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - glom
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    // List => Int
    // Int => Array
    val glomRDD: RDD[Array[Int]] = rdd.glom()

    glomRDD.collect().foreach((data: Array[Int]) => println(data.mkString(",")))


    sc.stop()
  }
}
