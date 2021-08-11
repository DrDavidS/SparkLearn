package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * P74 https://www.bilibili.com/video/BV11A411L7CK?p=74
 * KV类型
 * reduceByKey
 * aggregateByKey
 * foldByKey
 * combineByKey
 * 的区别
 */

object Spark20_RDD_Operator_Transform3 {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - KV类型 combineByKey

    val rdd: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("a", 1), ("a", 2), ("b", 3),
        ("b", 4), ("b", 5), ("a", 6)
      ), 2)

    // 下面四种其实都是wordcount效果
    /*
    reduceByKey:
        combineByKeyWithClassTag[V](
                (v: V) => v,  // 没有初始值，第一个值不会参与计算
                func,         // 分区内数据的处理函数
                func,         // 分区间数据的处理函数，注意两者完全一样
                partitioner)

    aggregateByKey:
        combineByKeyWithClassTag[U](
                (v: V) => cleanedSeqOp(createZero(), v),
                cleanedSeqOp, // 分区内数据的处理函数
                combOp,       // 分区间数据的处理函数
                partitioner)

    foldByKey:
        combineByKeyWithClassTag[V](
                (v: V) => cleanedFunc(createZero(), v),  // 初始值和第一个key的value值进行的分区内数据操作
                cleanedFunc,  // 分区内数据的处理函数
                cleanedFunc,  // 分区间数据的处理函数，注意和上面一个是一样的，所以简化了
                partitioner)

    combineByKey:
        combineByKeyWithClassTag(
                createCombiner,   // 相同Key的第一条数据进行的处理
                mergeValue,       // 分区内数据的处理函数
                mergeCombiners,   // 分区间数据的处理函数
                partitioner,
                mapSideCombine,
                serializer)(null)
     */
    rdd.reduceByKey(_ + _) // wordCount
    rdd.aggregateByKey(0)(_ + _, _ + _) // wordCount
    rdd.foldByKey(0)(_ + _) // wordCount
    rdd.combineByKey(v => v, (x: Int, y: Int) => x + y, (x: Int, y: Int) => x + y) // wordCount


    sc.stop()
  }
}

