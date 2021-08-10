package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date

/**
 * P55 https://www.bilibili.com/video/BV11A411L7CK?p=55
 * filter 的用法
 *
 *  数据筛选过滤后，分区不变，但是生产环境下可能会有数据倾斜
 */

object Spark07_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - filter
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    val filterRDD: RDD[Int] = rdd.filter(num => num % 2 != 0)
    
    filterRDD.collect().foreach(println)
    
    sc.stop()
  }
}
