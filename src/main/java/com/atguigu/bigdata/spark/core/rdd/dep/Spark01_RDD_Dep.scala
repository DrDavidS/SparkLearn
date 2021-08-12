package com.atguigu.bigdata.spark.core.rdd.dep

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * P93 RDD的血缘关系
 *
 * 利用 toDebugString 来打印血缘关系
 *
 * 注意调用了 Shuffle 的情况会中断一下
 *
 */

object Spark01_RDD_Dep {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)


    val lines: RDD[String] = sc.textFile("datas/word.txt")
    println(lines.toDebugString)
    println("********************")

    val words: RDD[String] = lines.flatMap((line: String) => line.split(" "))
    println(words.toDebugString)
    println("********************")

    val word2one: RDD[(String, Int)] = words.map(
      (word: String) => (word, 1)
    )
    println(word2one.toDebugString)
    println("********************")

    val word2Sum: RDD[(String, Int)] = word2one.reduceByKey(_ + _)
    println(word2Sum.toDebugString)
    println("********************")

    val array: Array[(String, Int)] = word2Sum.collect()
    array.foreach(println)

    sc.stop()
  }
}
