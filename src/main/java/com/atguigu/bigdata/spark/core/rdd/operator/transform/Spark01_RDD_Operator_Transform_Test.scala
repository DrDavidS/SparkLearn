package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark尝试读取文件，Split取特定位置
 */

object Spark01_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - map
    val rdd: RDD[String] = sc.textFile("datas/1.txt")
    val mapRDD: RDD[String] = rdd.map(
      line => {
        val datas = line.split(" ")  // 空格分割
        datas(1)
      })

    mapRDD.collect().foreach(println)
    sc.stop()
  }
}
