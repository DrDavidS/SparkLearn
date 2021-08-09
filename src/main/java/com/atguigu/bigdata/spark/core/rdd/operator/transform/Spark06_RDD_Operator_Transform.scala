package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * glom 的用法
 *
 * 一个分区的数据形成一个数组
 * 现在看看例子
 */

object Spark06_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - groupBy
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    def groupFunction(num: Int): Int = {
      num % 2
    }

    val groupRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(groupFunction)

    groupRDD.collect().foreach(println)
    sc.stop()
  }
}
