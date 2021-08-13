package com.atguigu.bigdata.spark.core.rdd.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * P94-P96 RDD的血缘关系-阶段划分
 *
 * dependencies
 *
 * 打印依赖，注意依赖关系的区别：
 *
 * OneToOneDependency：可以看看源码，继承了一个窄依赖
 * 窄依赖的数据分区是一对一的
 *
 * ShuffleDependency：宽依赖
 *
 * 这里涉及了一个阶段（stage）的概念，窄依赖的东西就在一个stage里面
 * 宽依赖就分为多个stage，一个stage的东西执行完才进入下一个stage，具体的可以看看P95-96的内容
 *
 * 当RDD存在Shuffle依赖的时候，阶段会自动增加一个
 * 阶段的数量 = shuffle依赖的数量 + 1
 *
 * ResultStage只有一个，最后需要执行的阶段
 */

object Spark02_RDD_Dep {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)


    val lines: RDD[String] = sc.textFile("datas/word.txt")
    println(lines.dependencies)
    println("********************")

    val words: RDD[String] = lines.flatMap((line: String) => line.split(" "))
    println(words.dependencies)
    println("********************")

    val word2one: RDD[(String, Int)] = words.map(
      (word: String) => (word, 1)
    )
    println(word2one.dependencies)
    println("********************")

    val word2Sum: RDD[(String, Int)] = word2one.reduceByKey(_ + _)
    println(word2Sum.dependencies)
    println("********************")

    val array: Array[(String, Int)] = word2Sum.collect()
    array.foreach(println)

    sc.stop()
  }
}
