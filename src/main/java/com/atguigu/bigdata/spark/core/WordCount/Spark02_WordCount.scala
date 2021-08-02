package com.atguigu.bigdata.spark.core.WordCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount {
  def main(args: Array[String]): Unit = {
    // Application02
    // Spark 框架
    // 一、建立和 Spark 框架的连接
    //JDBC: Connection
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    // 二、执行业务操作

    val lines: RDD[String] = sc.textFile("datas")

    val words: RDD[String] = lines.flatMap(line => line.split(" "))

    val word2one: RDD[(String, Int)] = words.map(
      word => (word, 1)
    )

    val wordGroup: RDD[((String, Int), Iterable[(String, Int)])] = word2one.groupBy(word => word)

    // wordGroup.foreach(println)


    val word2Count: RDD[(String, Int)] = wordGroup.map {
          // 这里WORD是一个key，没用了
      case (word: (String, Int), count: Iterable[(String, Int)]) =>
        count.reduce(
          (t1, t2) => {
            (t1._1, t1._2 + t2._2)
          }
        )
    }

    // 最后打印运行结果
    val array2: Array[(String, Int)] = word2Count.collect()
    array2.foreach(println)

    // 三、关闭连接
    sc.stop()
  }
}
