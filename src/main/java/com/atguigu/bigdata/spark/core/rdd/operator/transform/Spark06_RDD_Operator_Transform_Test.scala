package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date

/**
 * P54 https://www.bilibili.com/video/BV11A411L7CK?p=54
 * groupBy 的用法
 *
 * 按首字母来分组，现在看看例子
 */

object Spark06_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - groupBy
    val rdd: RDD[String] = sc.textFile("datas/apache.log")

    val timeRDD: RDD[(String, Iterable[(String, Int)])] = rdd.map(
      (line: String) => {
        val data: Array[String] = line.split(" ")
        val time: String = data(3) // 日期
        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")  // 转换日期
        val date: Date = sdf.parse(time)
        val sdf1 = new SimpleDateFormat("HH")  // 只取小时
        val hour: String = sdf1.format(date)
        (hour, 1)  // 小时内一次访问算一次
      }
    ).groupBy(_._1) // group by hour

    val mapRDD: RDD[(String, Int)] = timeRDD.map(
      (tuple: (String, Iterable[(String, Int)]))
      => (tuple._1, tuple._2.size)  // 这里取的是size，其实也可以用reduce相加
    )
    mapRDD.collect().foreach(println)

    sc.stop()
  }
}
