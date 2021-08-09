package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * mapPartitions
 */

object Spark02_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - map
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    /* mapPartitions
        可以以分区为单位进行数据的转换操作
        但是会将整个分区的数据加载到内存进行引用
        如果处理完的数据是不会被释放掉的，因为存在对象引用
        如果内存比较小而数据量比较大，容易内存溢出，用map更好

     */
    val mapRDD: RDD[Int] = rdd.mapPartitions(
      (iter: Iterator[Int]) => {
        println(">>>>>>>>>>>>>>>>>>")
        iter.map(_ * 2)
      }
    )

    mapRDD.collect().foreach(println)

    sc.stop()
  }
}
