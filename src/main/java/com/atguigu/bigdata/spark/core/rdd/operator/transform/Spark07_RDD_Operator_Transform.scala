package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * P56 https://www.bilibili.com/video/BV11A411L7CK?p=56
 * sample 的用法
 *
 */

object Spark07_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - sample
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))


    // sample 需要三个参数
    rdd.sample(
      withReplacement = false, // 1. 是否要放回
      0.4, // 2. 如果是抽取不放回：数据源中每条数据被抽取的概率。
                    // 如果是抽取放回，表示数据源中每条数据被抽取的可能次数
      888 // 3. 随机种子，如果不传就是系统时间
    ).collect().foreach(println)

    sc.stop()
  }
}
