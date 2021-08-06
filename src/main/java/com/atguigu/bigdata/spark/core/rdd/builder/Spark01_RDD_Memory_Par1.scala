package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 从内存中创建RDD，如果数据少而分区多会怎么样？怎么实现的？
 */

object Spark01_RDD_Memory_Par1 {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    // 这里的*的意思是采用最大CPU核
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // 2. 创建RDD

    // 【1，2】，【3，4】
    // val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // 【1】，【2】，【3，4】
    // val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 3)

    // 【1】，【2，3】，【4，5】
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 3)  // 深入看源码

    // 将处理的数据保存成分区文件
    rdd.saveAsTextFile("output")


    // 3. 关闭环境
    sc.stop()
  }
}
