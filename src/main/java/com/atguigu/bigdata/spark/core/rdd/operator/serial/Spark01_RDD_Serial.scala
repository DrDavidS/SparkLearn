package com.atguigu.bigdata.spark.core.rdd.operator.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * P90 序列化
 *
 */

object Spark01_RDD_Serial {
  def main(args: Array[String]): Unit = {
    // 环境准备
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))

    val search = new Search("h")
    //search.getMatch1(rdd).collect().foreach(println)
    search.getMatch2(rdd).collect().foreach(println)

    sc.stop()
  }

  // 查询对象
  // 类的构造参数其实是类的属性
  // query 就是构造参数，同时也是属性
  // 所以 构造参数需要进行闭包检测，等同于类进行闭包检测
  class Search(query: String) extends Serializable{
    def isMatch(s: String): Boolean = {
      s.contains(query)
    }

    // 函数序列化案例
    def getMatch1(rdd: RDD[String]): RDD[String] = {
      rdd.filter(isMatch)
    }


    // 属性序列化案例
    def getMatch2(rdd: RDD[String]): RDD[String] = {
      rdd.filter(_.contains(query))
    }

  }
}
