package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 从硬盘文件中创建RDD
 */

object Spark02_RDD_File1 {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    /* 2. 创建RDD
    * textFile 方法是以行为单位读取数据的
    *
    * wholeTextFiles 方法是以文件为单位读取数据
    * 读出来是元组，第一个元素是文件路径，第二个元素是文件内容
    */
    val rdd: RDD[(String, String)] = sc.wholeTextFiles("datas")
    rdd.collect().foreach(println)

    // 3. 关闭环境
    sc.stop()
  }
}
