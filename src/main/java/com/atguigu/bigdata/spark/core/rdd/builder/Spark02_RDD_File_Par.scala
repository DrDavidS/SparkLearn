package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 从硬盘文件中创建RDD，数据应当有几个分区
 */

object Spark02_RDD_File_Par {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    /* 2. 创建RDD
    *   textFile可以将文件作为数据处理的数据源，默认也可以设定分区
    *     minPartitions: 最小分区数量
    *     math.min(defaultParallelism, 2)  取最小的那个
    * val rdd: RDD[String] = sc.textFile("datas/1.txt")
    * 如果不想使用默认的分区数量，可以通过第二个参数指定分区数
    *
    * SparK读取文件，底层使用的就是Hadoop的读取方式
    * 分区数量的计算方式：
    *  totalSize = 7  (字节)
    *  goalSize = 7 / 2 = 3 (字节)
    *
    * 7 / 3 = 2 ... 1 超过了10%所以+1，所以是3个分区，这是Hadoop的东西
    */
    val rdd: RDD[String] = sc.textFile("datas/1.txt", 3)


    // 3. 关闭环境
    sc.stop()
  }
}
