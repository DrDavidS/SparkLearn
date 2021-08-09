package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * flatMap
 *
 */

object Spark04_RDD_Operator_Transform2 {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - flatMap
    val rdd: RDD[Any] = sc.makeRDD(List(List(1, 2), 3, List(4, 5))) // 混合类型
    // 因此我们在这里模式匹配
    val flatRDD: RDD[Any] = rdd.flatMap {
      case list: List[Int] => list
      case dat => List(dat)
    }

    flatRDD.collect().foreach(println)

    sc.stop()
  }
}
