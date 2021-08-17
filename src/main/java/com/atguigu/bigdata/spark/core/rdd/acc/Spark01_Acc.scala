package com.atguigu.bigdata.spark.core.rdd.acc

/**
 * P105 累加器
 */

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Acc {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // reduce 包括分区内的计算和分区间的计算
    //    val i: Int = rdd.reduce(_ + _)
    //    println(i)

    // 下面这段代码其实sum不是10，而是0
    //    var sum = 0
    //    rdd.foreach(
    //      num => {
    //        sum += num
    //      }
    //    )
    //    println("sum = " + sum)

    // 我们应该使用累加器

    sc.stop()
  }
}
