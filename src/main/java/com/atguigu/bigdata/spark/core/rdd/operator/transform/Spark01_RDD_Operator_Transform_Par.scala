package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 这里看看线程的并发执行，指定1分区和不指定分区，执行顺序有什么区别
 */
object Spark01_RDD_Operator_Transform_Par {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - map

    /*
      1.  RDD 的计算一个分区内的数据是一个一个执行逻辑
          只有前面一个数据全部的逻辑执行完毕后才会执行下一个数据
          分区内数据是有序的
      2.  不同分区数据计算是无序的
     */
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)  // 如果不是1个分区，就是并行的
    val mapRDD: RDD[Int] = rdd.map(
      num => {
        println(">>>>>>> " + num)
        num
      }
    )

    val mapRDD1 = mapRDD.map(
      num => {
        println("####### " + num)
        num
      }
    )

    mapRDD1.collect()
    sc.stop()
  }
}
