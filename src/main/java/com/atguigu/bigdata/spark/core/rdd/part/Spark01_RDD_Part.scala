package com.atguigu.bigdata.spark.core.rdd.part

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

// 分区P103

object Spark01_RDD_Part {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String, String)] = sc.makeRDD(List(
      ("nba", "xxxxxx"),
      ("cba", "xxxxxx"),
      ("wnba", "xxxxxx"),
      ("nba", "xxxxxx")
    ))

    val partRDD: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner)

    partRDD.saveAsTextFile("output")

    sc.stop()
  }
}

/*
 *   自定义分区器
 * 1. 继承 Partitioner
 * 2. 重写方法
 */

class MyPartitioner extends Partitioner {
  // 分区数量
  override def numPartitions: Int = 3

  // 返回数据的分区索引，从0开始
  override def getPartition(key: Any): Int = {
    key match {
      case "nba" => 0
      case "wnba" => 1
      case _ => 2
    }
  }
}