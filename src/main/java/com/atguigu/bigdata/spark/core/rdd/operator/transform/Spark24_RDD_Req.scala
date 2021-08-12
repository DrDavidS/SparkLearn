package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * P78-80 https://www.bilibili.com/video/BV11A411L7CK?p=77
 *
 * 案例实操
 *
 * 数据准备：
 * agent.log：时间戳，身份，城市，用户，广告，中间用空格分隔
 *
 * 需求描述：
 * 统计出每个省份每个广告被点击数量的排行Top3
 *
 */

object Spark24_RDD_Req {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    // TODO 案例实操

    // 1. 获取原始数据
    val dataRDD: RDD[String] = sc.textFile("datas/agent.log")

    // 2. 原始数据结构转换，方便统计
    //    时间戳，身份，城市，用户，广告 => ((省份， 广告), 1)
    val mapRDD: RDD[((String, String), Int)] = dataRDD.map(
      line => {
        val data: Array[String] = line.split(" ")
        ((data(1), data(4)), 1)
      }
    )

    // 3. 将转换结构后的数据，进行分组聚合
    //    ((省份， 广告), 1) => ((省份， 广告), sum)
    val reduceRDD: RDD[((String, String), Int)] = mapRDD.reduceByKey(_ + _)

    // 4. 将聚合结果进行结构转换
    //    ((省份， 广告), sum) => (省份， (广告, sum))

    //    val newMapRDD: RDD[(String, (String, Int))] = reduceRDD.map(
    //      (tuple: ((String, String), Int)) => {
    //        (tuple._1._1, (tuple._1._2, tuple._2))
    //      }
    //    )

    val newMapRDD: RDD[(String, (String, Int))] = reduceRDD.map {
      case ((province, ad), sum) => {
        (province, (ad, sum))
      }
    }

    // 5. 转换结构后的数据根据省份分组
    //    (省份, 【(广告A, sum A), (广告B, sumB)】)
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = newMapRDD.groupByKey()

    // 6. 分组后的数据组内排序，降序，取前三名
    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
    )

    // 7. 输出结果
    resultRDD.collect().foreach(println)

    sc.stop()
  }
}

