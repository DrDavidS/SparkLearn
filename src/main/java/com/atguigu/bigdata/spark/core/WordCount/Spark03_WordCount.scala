package com.atguigu.bigdata.spark.core.WordCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_WordCount {
  def main(args: Array[String]): Unit = {
    // Application02
    // Spark 框架
    // 一、建立和 Spark 框架的连接
    //JDBC: Connection
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    // 二、执行业务操作

    val lines: RDD[String] = sc.textFile("datas")

    // val words: RDD[String] = lines.flatMap(line => line.split(" "))
    val words: RDD[String] = lines.flatMap(_.split(" "))

    val word2one: RDD[(String, Int)] = words.map(
      word => (word, 1)
    )

    // Spark框架提供了更多的功能，可以将分组和聚合使用一个方法实现
    // 相同的 key 的数据，可以对 value 进行 reduce 聚合
    // word2one.reduceByKey((x, y) => {x + y})
    val word2Count = word2one.reduceByKey(_ + _)

    // 最后打印运行结果
    val array3: Array[(String, Int)] = word2Count.collect()
    array3.foreach(println)

    // 三、关闭连接
    sc.stop()
  }
}
