package com.atguigu.bigdata.spark.core.rdd.acc

/**
 * P107-108
 * 自己实现一个累加器
 *
 */

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark04_Acc_WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(List("hello", "spark", "hello"))

    // rdd.map((_, 1)).reduceByKey(_ + _)

    // 自己实现一个累加器
    // 创建累加器对象
    val wcAcc = new MyAccumulator()
    // 向Spark进行注册
    sc.register(wcAcc)

    rdd.foreach(
      (word: String) => {
        // 使用累加器
        wcAcc.add(word)
      }
    )
    // 获取累加器累加的结果
    println(wcAcc.value)

    sc.stop()
  }
}


/*
自定义累加器的实现：
1. 继承AccumulatorV2，定义泛型
  IN: 累加器输入的数据类型
 OUT: 累加器需要返回的数据类型
2. 重写所有的方法
*/
class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {

  private val wcMap: mutable.Map[String, Long] = mutable.Map[String, Long]()

  // 判断当前是否为初始状态
  override def isZero: Boolean = {
    wcMap.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = new MyAccumulator()

  // 重置
  override def reset(): Unit = wcMap.clear()

  // 获取累加器需要计算的值
  override def add(word: String): Unit = {
    val newCnt: Long = wcMap.getOrElse(word, 0L) + 1
    wcMap.update(word, newCnt)
  }

  // Driver合并多个累加器(在这里就是两个map的合并)
  override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
    val map1: mutable.Map[String, Long] = this.wcMap
    val map2: mutable.Map[String, Long] = other.value

    map2.foreach {
      case (word, count) =>
        val newCount: Long = map1.getOrElse(word, 0L) + count
        map1.update(word, newCount)
    }
  }

  // 累加器结果
  override def value: mutable.Map[String, Long] = wcMap
}