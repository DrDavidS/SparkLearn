package NormalGraphX

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import ReadDataOnHive.TailFact

/**
 * Graph Demo 17 优化版本
 *
 * 本章节代码的目标是，做一次发送前后对比，如果本次和上次的Map完全一致，则不发送消息。
 *
 */

object G01_calculateStockRatio_MsgSendOnce {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkLocalConf().sc

    // 默认投资信息
    val defaultVertex = Map(99999L -> 0.00)

    /* 创建边关系，边的属性就是投资金额
    * 边的格式： Edge(srcId, dstId, attr)
    */
    val shareEdgeSeq = Seq(
      Edge(1L, 6L, 1.0), // 青毛狮子怪 -> 狮驼岭集团
      Edge(6L, 4L, 1.0), // 狮驼岭集团 -> 右护法
      Edge(6L, 3L, 1.0), // 狮驼岭集团 -> 左护法
      Edge(4L, 5L, 0.675325), // 右护法 -> 小钻风
      Edge(3L, 5L, 0.324675) // 左护法 -> 小钻风
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
    println("进入多层计算 -> loop 10")
    val ShareHoldingGraph: Graph[Map[VertexId, Double], Double] = TailFact.simpleTailFact(20, initGraph) // 理论上递归次数增加不影响结果才是对的
    println("=======  ShareHoldingGraph  ==========")
    ShareHoldingGraph.vertices.collect.foreach(println)
  }
}
