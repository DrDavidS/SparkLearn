package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * P88 行动算子
 *
 * foreach
 *
 * 注意 collect 是排序了的，而单独的foreach是没有顺序的
 * 算子:Operator
 * RDD的方法和Scala集合对象的方法不一样
 * 集合对象的方法都是在同节点的内存中完成的
 * RDD的方法可以将计算逻辑发送到 Executor 端（分布式节点）执行
 * 为了区分不同的处理效果，所以把RDD的方法称之为算子
 *
 * RDD方法的外部操作（算子外部的操作）在Driver端，内部在 Executor 中执行
 * 这就导致算子内进程会用到算子外面的数据，就形成了闭包的效果
 * 会有一个闭包检测的过程
 */

object Spark07_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    // 环境准备
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    val user = new User()

    // Task not serializable
    // java.io.NotSerializableException:
    // com.atguigu.bigdata.spark.core.rdd.operator.action.Spark07_RDD_Operator_Action$User

    // 需要序列化
    // RDD 算子中传递的函数会包含闭包操作，那么会进行检测功能
    // 称为闭包检测功能
    rdd.foreach(num => {
      println("age = " + (user.age + num))
    })

    sc.stop()
  }

  // 使用 case Class 在编译的时候会自动混入序列化特质（实现可序列化接口）
  class User extends Serializable {
    val age: Int = 30
  }
}
