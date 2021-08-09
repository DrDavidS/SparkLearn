package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * mapPartitionsWithIndex
 * 尝试保留特定分区数据
 * 这里打印了各个数据所在的分区
 */

object Spark03_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - mapPartitionsWithIndex
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //【1，2】，【3，4】
    //【3，4】
    val mpiRDD: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        //1,2,3,4
        //(0,1),(2,2),(4,3),(6,4)
        iter.map(
          num => {
            (index, num)
          }
        )
      }
    )

    mpiRDD.collect().foreach(println)

    sc.stop()
  }
}
