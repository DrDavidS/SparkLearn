package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * P88 行动算子
 *
 * foreach
 *
 * 注意 collect 是排序了的，而单独的foreach是没有顺序的
 * 算子:Operator
 * RDD的方法和Scala集合对象的方法不一样
 * 集合对象的方法都是在同节点的内存中完成的
 * RDD的方法可以将计算逻辑发送到 Executor 端（分布式节点）执行
 * 为了区分不同的处理效果，所以把RDD的方法称之为算子
 *
 * RDD方法的外部操作在Driver端，内部在 Executor 中执行
 */

object Spark06_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    // 环境准备
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // TODO 行动算子
    rdd.collect().foreach(println) // 存在排序,实际上是在Driver端内存集合的循环遍历方法
    println("*********************")
    rdd.foreach(println) // 乱序，实际上是在 Executor 端内存数据打印

    sc.stop()
  }
}
