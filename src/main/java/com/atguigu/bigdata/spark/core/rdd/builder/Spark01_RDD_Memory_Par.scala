package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 从内存中创建RDD，设定多个分区的方法及原理
 */

object Spark01_RDD_Memory_Par {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    // 这里的*的意思是采用最大CPU核
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    sparkConf.set("spark.default.parallelism", "3")  // 3个分区，在配置文件里面指定
    val sc = new SparkContext(sparkConf)

    // 2. 创建RDD
    // RDD的并行 & 分区
    // makeRDD方法可以传递第二个参数，这个参数表示分区的数量
    // 第二个参数也可以不传递，那么makeRDD会使用默认值
    //   scheduler.conf.getInt("spark.default.parallelism", totalCores)
    //   spark在默认情况下，从配置对象中获取配置参数 spark.default.parallelism
    //   如果获取不到，那么使用totalCores属性，这个属性的取值为当前运行环境的最大可用核心数
    //val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2) // 分区数量
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // 将处理的数据保存成分区文件
    rdd.saveAsTextFile("output")


    // 3. 关闭环境
    sc.stop()
  }
}
