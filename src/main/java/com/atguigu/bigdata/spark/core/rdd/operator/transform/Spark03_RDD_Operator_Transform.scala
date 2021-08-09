package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * mapPartitionsWithIndex
 * 尝试保留特定分区数据
 */

object Spark03_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - mapPartitionsWithIndex
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    //【1，2】，【3，4】
    //【3，4】
    val mpiRDD: RDD[Int] = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        if (index == 1) {
          iter
        } else {
          Nil.iterator
        }
      }
    )

    mpiRDD.collect().foreach(println)

    sc.stop()
  }
}
