package com.atguigu.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * P102 RDD的持久化
 *
 * 方法对比：
 * cache：将数据临时存储在内存中进行数据重用
 *        会在血缘关系汇总添加新的依赖
 * persist：将数据临沭存储在磁盘文件中进行数据重用
 *          涉及磁盘IO，性能低，数据比较安全
 *          作业执行完毕，临时保存的数据文件就会丢失
 * checkpoint：数据长久保存在磁盘文件中进行数据重用
 *              涉及磁盘IO，性能低
 *             为了保证数据安全，一般情况下会独立执行数据作业
 *             等同于改变了数据源！
 */

object Spark06_RDD_Persist {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    sc.setCheckpointDir("cp") // 设置检查点路径

    val list = List("Hello Scala", "Hello Spark")

    val rdd: RDD[String] = sc.makeRDD(list)

    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))

    val mapRDD: RDD[(String, Int)] = flatRDD.map((_, 1))

    // mapRDD.cache()
    mapRDD.checkpoint()
    println(mapRDD.toDebugString)

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)

    println("************************")
    println(mapRDD.toDebugString) // 增加了依赖
    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()

    groupRDD.collect().foreach(println)

    sc.stop()
  }
}
