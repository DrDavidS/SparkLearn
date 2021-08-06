package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 从硬盘文件中创建RDD
 */

object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    /* 2. 创建RDD
    * 从文件中创建RDD，将文件中集合的数据作为处理的数据源
    * 相对路径、绝对路径都可以
    * val rdd: RDD[String] = sc.textFile("datas/1.txt")
    *
    * 如果读取多个文件，也可以给出文件目录名称
    * val rdd: RDD[String] = sc.textFile("datas")
    *
    * path路径还可以使用通配符*
    *
    *
    * 也可以是HDFS，甚至网络地址
    */
    val rdd: RDD[String] = sc.textFile("datas/1*.txt")  // 通配符
    rdd.collect().foreach(println)

    // 3. 关闭环境
    sc.stop()
  }
}
