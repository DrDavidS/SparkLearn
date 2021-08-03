package com.atguigu.bigdata.spark.core.WordCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * 这是比较精简版本的 WordCount 操作
 * RDD的数据处理方式类似于IO流，也有装饰者设计模式
 * RDD的数据只有在调用collect方法时，才会真正执行业务逻辑操作，之前的都是功能的扩展
 *
 * 其中RDD包装顺序如下：
 * RDD - 对应函数
 * 1. HadoopRDD - textFile
 * 2. MapPartitionsRDD - flatMap
 * 3. MapPartitionsRDD - map
 * 4. ShuffledRDD - reduceByKey
 *
 * 此外RDD中间不保存数据
 */

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
