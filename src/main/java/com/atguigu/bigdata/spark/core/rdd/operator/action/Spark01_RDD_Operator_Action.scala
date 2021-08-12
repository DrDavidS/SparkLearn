package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * P81 行动算子
 *
 * collect()
 * 所谓行动算子，就是触发作业执行的方法
 *
 */

object Spark01_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    // 环境准备
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // TODO 行动算子
    // 底层代码调用runJob方法
    // 底层代码中会创建 ActiveJob，创建作业
    rdd.collect()

    sc.stop()
  }
}
