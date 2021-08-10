package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * P63 https://www.bilibili.com/video/BV11A411L7CK?p=63
 * KV类型
 * HashPartitioner
 * RangePartitioner
 *
 *
 * 如果按照自己的方法进行数据分区——自己写一个分区器
 */

object Spark14_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - KV类型

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    val mapRDD: RDD[(Int, Int)] = rdd.map((_, 1)) // 变成元组，有KV性质了
    // RDD => PairRDDFunctions
    // 隐式转换(二次编译)
    // rddToPairRDDFunctions

    // partitionBy根据指定的分区规则对数据重新分区
    // 默认就是 HashPartitioner
    val newRDD: RDD[(Int, Int)] = mapRDD.partitionBy(new HashPartitioner(2))
    // newRDD.partitionBy(new HashPartitioner(2))  // 如果一样，不会做什么操作


    sc.stop()
  }
}
