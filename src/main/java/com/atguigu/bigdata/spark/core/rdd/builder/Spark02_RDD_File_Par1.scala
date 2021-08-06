package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 从硬盘文件中创建RDD，数据分区的分配
 */

object Spark02_RDD_File_Par1 {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // 2. 创建RDD

    // 3. 数据分区的分配

    // 1.数据以行为单位进行读取
    //   Spark读取文件，采用的是Hadoop方式读取，是一行一行读取，和字节数没有关系
    // 2.数据读取时以偏移量为单位，就是算上换行符什么的，偏移量不会被重复读取
    // 3.数据分区的偏移量的计算范围
    // 0 => [0, 3]  // 这里有回车换行符
    // 1 => [3, 6]  // 这里有回车换行符
    // 2 => [6, 7]  // 没了
    val rdd: RDD[String] = sc.textFile("datas/1.txt", 3)


    // 4. 关闭环境
    sc.stop()
  }
}
