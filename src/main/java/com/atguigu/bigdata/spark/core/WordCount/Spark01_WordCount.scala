package com.atguigu.bigdata.spark.core.WordCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
    // Application
    // Spark 框架
    // 一、建立和 Spark 框架的连接
    //JDBC: Connection
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    // 二、执行业务操作

    // 1. 读取文件，获取一行一行的数据
    // hello world
    val lines: RDD[String] = sc.textFile("datas")

    // 2. 将一行数据进行拆分，形成一个一个的单词（分词）
    // 扁平化操作
    // "hello world" -> hello, world, hello, world
    val words: RDD[String] = lines.flatMap(line => line.split(" "))

    // 3. 将数据根据单词进行分组，便于统计
    // (hello, hello, hello), (word, word)
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)


    // 4. 对分组后的数据进行转换
    // (hello, hello, hello), (word, word) -> (hello, 3), (world, 2)
    val word2Count = wordGroup.map {
      case (word, list) => (word, list.size)
    }
    // 5. 将转换结果采集到控制台打印出来
    val array: Array[(String, Int)] = word2Count.collect()
    array.foreach(println)

    // 三、关闭连接
    sc.stop()
  }
}
