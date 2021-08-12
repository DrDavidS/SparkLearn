package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * P82 行动算子
 *
 * 所谓行动算子，就是触发作业执行的方法
 *
 */

object Spark02_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    // 环境准备
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // TODO 行动算子

    // 1. reduce
    //    val i: Int = rdd.reduce(_ + _)
    //    println(i)

    // 2. collect
    // 将不同分区数据按照分区顺序采集到Driver端内存中，形成数组
    //    val ints: Array[Int] = rdd.collect()
    //    println(ints.mkString(","))

    // 3. count
    // 数据源中数据的个数
    val cnt: Long = rdd.count()
    println(cnt)

    // 4. first
    // 获取数据源中数据的第一个
    val first: Int = rdd.first()
    println(first)

    // 5. take
    // 获取N个数据
    val takeRes: Array[Int] = rdd.take(3)
    println(takeRes.mkString(","))

    // 6. takeOrdered
    // 排序获取N个数据
    val rdd_disorder: RDD[Int] = sc.makeRDD(List(4, 2, 3, 1))
    val ints: Array[Int] = rdd_disorder.takeOrdered(3)
    println(ints.mkString(","))

    sc.stop()
  }
}
