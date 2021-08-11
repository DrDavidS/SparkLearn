package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * P70 https://www.bilibili.com/video/BV11A411L7CK?p=70
 * KV类型
 *
 *
 */

object Spark18_RDD_Operator_Transform3 {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - KV类型 aggregateByKey

    val rdd: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("a", 1), ("a", 2), ("b", 3),
        ("b", 4), ("b", 5), ("a", 6)
      ), 2)

    // aggregateByKey 最终的返回数据结果应该和初始值的类型保持一致
    //    val aggRDD: RDD[(String, Int)] = rdd.aggregateByKey(0)(_ + _, _ + _)
    //    aggRDD.collect().foreach(println)

    // 获取相同key的数据的平均值  => (a, 3), (b, 4)
    // 做一个元组(t, v)，其中 v 代表次数
    val newRDD: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(
      (t: (Int, Int), v: Int) => {
        (t._1 + v, t._2 + 1)
      },
      (t1: (Int, Int), t2: (Int, Int)) => {
        (t1._1 + t2._1, t1._2 + t2._2)
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

