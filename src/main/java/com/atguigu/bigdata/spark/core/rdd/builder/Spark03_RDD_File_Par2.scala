package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 从硬盘文件中创建RDD，数据分区的分配
 */

object Spark03_RDD_File_Par2 {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // 2. 创建RDD

    // 14byte / 2 = 7byte
    // 14 / 7 = 2 分区
    /* 看看偏移量

      1234567   => 012345678 （加上回车换行）
      89        => 9 10 11 12 （加上回车换行）
      0         => 13
     */

    // 如果数据源为多个文件，那么计算分区时以文件为单位
    val rdd: RDD[String] = sc.textFile("datas/word.txt", 2)

    rdd.saveAsTextFile("output")



    // 3. 关闭环境
    sc.stop()
  }
}
