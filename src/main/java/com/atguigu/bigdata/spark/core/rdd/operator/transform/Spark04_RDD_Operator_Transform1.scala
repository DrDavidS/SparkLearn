package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * flatMap
 *
 */

object Spark04_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - flatMap
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val flatRDD: RDD[String] = rdd.flatMap(
      s => {
        s.split(" ")
      }
    )

    flatRDD.collect().foreach(println)

    sc.stop()
  }
}
