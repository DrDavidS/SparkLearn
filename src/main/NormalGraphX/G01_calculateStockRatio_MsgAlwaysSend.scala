package NormalGraphX

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import ReadDataOnHive.TailFact

/**
 * Graph Demo 16 再次优化版本
 *
 * 究极简化版，基于沐页的代码改写。看看能不能优化，尤其是发送消息那里。
 *
 * 改进：
 * 1. 去掉了一切var变量，统统使用val
 *
 * 2. 添加了很多注释、改写变量名，便于理解
 *
 * 3. 采用了尾递归调用，比For更节约资源
 *
 * 4. 简化了数据结构，大幅降低计算量
 *
 */

object G01_calculateStockRatio_MsgAlwaysSend {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkLocalConf().sc

    // 默认投资信息
    val defaultVertex = Map(99999L -> 0.00)

    /* 创建边关系，边的属性就是投资金额
    * 边的格式： Edge(srcId, dstId, attr)
    */
    val shareEdgeSeq = Seq(
      Edge(1L, 3L, 0.5), // 青毛狮子怪 -> 左护法
      Edge(2L, 4L, 0.5), // 大鹏金翅雕 -> 左护法
      Edge(3L, 4L, 0.5),
      Edge(4L, 3L, 0.5)
    )
    val shareEdgeRDD: RDD[Edge[Double]] = sc.parallelize(shareEdgeSeq)
    // 构建初始图
    val rawGraph: Graph[Map[VertexId, Double], Double] = Graph.fromEdges(shareEdgeRDD, defaultVertex)

    // 发送并且聚合消息
    val initVertex: VertexRDD[Map[VertexId, Double]] = rawGraph.aggregateMessages[Map[VertexId, Double]](
      (triplet: EdgeContext[Map[VertexId, Double], Double, Map[VertexId, Double]]) => {
        val ratio: Double = triplet.attr // 持股比例
        val dstId: VertexId = triplet.dstId // 目标ID
        val vData = Map(dstId -> ratio)

        triplet.sendToSrc(vData)
      },
      // Merge Message
      _ ++ _
    )

    // join 初始化图
    val initGraph: Graph[Map[VertexId, Double], Double] = rawGraph.outerJoinVertices(initVertex)(
      (_: VertexId,
       _: Map[VertexId, Double],
       nvdata: Option[Map[VertexId, Double]]) => {
        nvdata.getOrElse(Map.empty)
      })

    //initGraph.vertices.collect.foreach(println)
    println("进入多层计算")
    // 尾递归：理论上递归次数增加不影响结果才是对的
    val ShareHoldingGraph: Graph[Map[VertexId, Double], Double] = TailFact.simpleTailFact(20, initGraph)
    println("=======  ShareHoldingGraph  ==========")
    ShareHoldingGraph.vertices.collect.foreach(println)
  }
}
