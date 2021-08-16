package com.atguigu.bigdata.spark.core.rdd.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * P104 IO 相关
 */

object Spark01_RDD_IO_Load {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val rdd1: RDD[String] = sc.textFile("output1")
    println(rdd1.collect().mkString(","))

    val rdd2: RDD[(String, Int)] = sc.objectFile[(String, Int)]("output2")
    println(rdd2.collect().mkString(","))


    val rdd3: RDD[(String, Int)] = sc.sequenceFile[String, Int]("output3")
    println(rdd3.collect().mkString(","))

    sc.stop()
  }
}
