package com.atguigu.bigdata.spark.core.rdd.acc

/**
 * P109 广播
 * join会导致数据量几何增长，还会影响shuffle性能，不推荐使用
 * 这里我们用一种特殊的功能代替了join，而且没有shuffle
 * 但是：闭包数据都是以task为单位发送的，每个任务包含闭包数据
 *      这样可能会导致一个Executor中包含大量重复数据，会占用大量内存
 *      Executor其实就是一个JVM，所以在启动的时候，会自动分配内存
 *      完全可以将任务中的闭包数据放在Executor的内存中，达到共享的目的【注意，不能更改数据】
 *
 *      Spark中的广播变量就可以将闭包中的数据保存到Excutor的内存中
 *      注意：广播变量不能更改，只能读取
 */

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark05_Bc {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 3)
    ))

    val map1: mutable.Map[String, Int] = mutable.Map(("a", 4), ("b", 5), ("c", 6))

    //    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(
    //      ("a", 4), ("b", 5), ("c", 6)
    //    ))

    //    val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
    //    joinRDD.collect().foreach(println)

    rdd1.map {
      case (word, count) =>
        val l: Int = map1.getOrElse(word, 0)  // 获取map1中的count，如果没有就是0
        (word, (count, l))
    }.collect().foreach(println)

    sc.stop()
  }
}