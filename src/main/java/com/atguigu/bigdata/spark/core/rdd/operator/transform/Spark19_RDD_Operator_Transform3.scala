package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * P70 https://www.bilibili.com/video/BV11A411L7CK?p=70
 * KV类型 combineByKey
 *
 *
 */

object Spark19_RDD_Operator_Transform3 {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - KV类型 combineByKey

    val rdd: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("a", 1), ("a", 2), ("b", 3),
        ("b", 4), ("b", 5), ("a", 6)
      ), 2)


    //combineByKey
    // 有三个参数
    // 第1个是：相同key的第一个数据进行结构的转换，实现操作
    // 第2个是：分区内的计算规则
    // 第3个是：分区间的计算规则
    val newRDD: RDD[(String, (Int, Int))] = rdd.combineByKey(
      (v: Int) => (v, 1),  // 相同key的第一个数据进行结构的转换，实现操作
      (t: (Int, Int), v: Int) => {
        (t._1 + v, t._2 + 1)  // 分区内，前者数量相加，后者次数+1
      },
      (t1: (Int, Int), t2: (Int, Int)) => {
        (t1._1 + t2._1, t1._2 + t2._2)  // 分区间，前者数量相加，后者次数相加
      }
    )

    // newRDD.collect().foreach(println)

    // 三种写法，要习惯
    // 1. 模式匹配
    //    val resultRDD: RDD[(String, Int)] = newRDD.mapValues {
    //      case (num, cnt) => num / cnt
    //    }

    // 2. map
    //val resultRDD: RDD[(String, Int)] = newRDD.map(res => (res._1, res._2._1 / res._2._2))


    // 3. mapValues，用于存在KV的情况
    val resultRDD: RDD[(String, Int)] = newRDD.mapValues((res: (Int, Int)) => res._1 / res._2)

    resultRDD.collect().foreach(println)

    sc.stop()
  }
}

