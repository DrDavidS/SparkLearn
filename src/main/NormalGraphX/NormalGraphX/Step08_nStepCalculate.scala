package NormalGraphX

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
 * Step08 多次调用
 *
 * 基于 Step07 的代码，如果我们想要计算更多层持股信息，就可以直接沿用相关代码，多复制粘贴几次就行了。
 * 更妙的方法是，将 aggregateMessages 和 aggregateMessages 打包为一个函数，然后重复调用。
 *
 * */

object Step08_nStepCalculate {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkLocalConf().sc

    val vertexSeq = Seq(
      (1L, ("青毛狮子怪", Map(6L -> 1.0))),
      (3L, ("左护法", Map(5L -> 0.324675))),
      (4L, ("右护法", Map(5L -> 0.675325))),
      (5L, ("小钻风", Map(999999999L -> 0.0))),
      (6L, ("狮驼岭集团", Map(3L -> 1.0, 4L -> 1.0)))
    )
    val vertexSeqRDD: RDD[(VertexId, (String, Map[VertexId, Double]))] = sc.parallelize(vertexSeq)

    val shareEdgeSeq = Seq(
      Edge(1L, 6L, 1.00), // 青毛狮子怪 -> 狮驼岭集团
      Edge(6L, 4L, 1.00), // 狮驼岭集团 -> 右护法
      Edge(6L, 3L, 1.00), // 狮驼岭集团 -> 左护法
      Edge(4L, 5L, 0.675325), // 右护法 -> 小钻风
      Edge(3L, 5L, 0.324675) // 左护法 -> 小钻风
    )
    val shareEdgeRDD: RDD[Edge[Double]] = sc.parallelize(shareEdgeSeq)

    // 构建初始图
    val rawGraph: Graph[(String, Map[VertexId, Double]), Double] = Graph(vertexSeqRDD, shareEdgeRDD)

    /**
     * n阶计算函数，用于递归调用。
     *
     * @param inGraph   输入的图，节点上是当前持股信息。
     * @param initGraph 初始图，只包含初始信息
     * @return 输出的图。包含了向下一层计算的持股信息。
     */
    def nStepCalculate(inGraph: Graph[(String, Map[VertexId, Double]), Double],
                       initGraph: Graph[(String, Map[VertexId, Double]), Double]
                      ): Graph[(String, Map[VertexId, Double]), Double] = {
      val capitalOfVertex: VertexRDD[Map[VertexId, Double]] = inGraph.aggregateMessages[Map[VertexId, Double]](
        (triplet: EdgeContext[(String, Map[VertexId, Double]), Double, Map[VertexId, Double]]) => {
          val dstShareHoldingMap: Map[VertexId, Double] = triplet.dstAttr._2 // 目标点当前的持股Map
          val ratio: Double = triplet.attr // 源点对目标点的持股比例

          val vertexShareHoldingMap: Map[VertexId, Double] = dstShareHoldingMap.map(
            (kv: (VertexId, Double)) => {
              (kv._1, kv._2 * ratio)
            })

          triplet.sendToSrc(vertexShareHoldingMap)
        },
        // mergeMsg
        // 这里用 _ ++ _ 会出问题
        // 转而采用 同key 相加的方法
        (leftMap, rightMap) => {
          val reduceLeftAndRightMap = leftMap ++ rightMap.map {
            case (k: VertexId, v: Double) =>
              // 左右投资比例，同key相加
              val sumRatio: Double = {
                v + leftMap.getOrElse(k, 0.00)
              }
              k -> sumRatio
          }
          reduceLeftAndRightMap
        }
      )

      val outGraph: Graph[(String, Map[VertexId, Double]), Double] = initGraph.outerJoinVertices(capitalOfVertex)(
        (_: VertexId, vdata: (String, Map[VertexId, Double]), nvdata: Option[Map[VertexId, Double]]) => {
          val newVertexDataMap: Map[VertexId, Double] = nvdata.getOrElse(Map(999999999L -> 0.00))
          (vdata._1, vdata._2 ++ newVertexDataMap) // (名字，Map 合并）
        }
      )
      outGraph
    }

    val newGraph2: Graph[(String, Map[VertexId, Double]), Double] = nStepCalculate(rawGraph, rawGraph)
    val newGraph3: Graph[(String, Map[VertexId, Double]), Double] = nStepCalculate(newGraph2, rawGraph)
    val newGraph4: Graph[(String, Map[VertexId, Double]), Double] = nStepCalculate(newGraph3, rawGraph)
    val newGraph5: Graph[(String, Map[VertexId, Double]), Double] = nStepCalculate(newGraph4, rawGraph)
    println("============== 5层 ====================")
    newGraph5.vertices.collect.foreach(println)
  }
}
