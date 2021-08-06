package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 从内存中创建RDD，原始玩法
 */

object Spark01_RDD_Memory {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    // 这里的*的意思是采用最大CPU核
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // 2. 创建RDD
    // 从内存中创建RDD，将内存中集合的数据作为处理的数据源
    val seq: Seq[Int] = Seq[Int](1, 2, 3, 4)

    // parallelize:并行
    // val rdd: RDD[Int] = sc.parallelize(seq)
    // makeRDD底层实现就是rdd对象的parallelize()方法
    val rdd: RDD[Int] = sc.makeRDD(seq)

    rdd.collect().foreach(println)
    // 3. 关闭环境
    sc.stop()
  }
}
