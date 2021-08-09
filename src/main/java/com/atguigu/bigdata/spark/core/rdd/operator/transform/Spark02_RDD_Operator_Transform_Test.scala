package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 练习：取出每个分区的最大值 by mapPartitions
 */

object Spark02_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - mapPartitions
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    //【1，2】，【3，4】
    // 【2】，【4】
    val mapRDD: RDD[Int] = rdd.mapPartitions(
      iter => {
        List(iter.max).iterator
      }
    )

    mapRDD.collect().foreach(println)

    sc.stop()
  }
}
