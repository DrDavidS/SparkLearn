import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
 * Step07 多边合并的情况
 *
 * 在 Step05 中， 我们计算了股东们对公司的持股比例。
 *
 * 但是如结尾所说，问题还是存在的。对于本例中的图，在 mergeMsg 阶段会出问题。
 *
 * 本节更换了对应的例子，以凸显 step05 代码中的问题。然后采用同key相加的方法，取代了直接合并Map的实现方式，
 * 以更好地应对复杂情况。
 *
 * */

object Step07_EdgeCombine {
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

    rawGraph.vertices.foreach(println)
    println("=========================")

    // 两步计算
    val capitalOfVertex: VertexRDD[Map[VertexId, Double]] = rawGraph.aggregateMessages[Map[VertexId, Double]](
      (triplet: EdgeContext[(String, Map[VertexId, Double]), Double, Map[VertexId, Double]]) => {
        val dstShareHoldingMap: Map[VertexId, Double] = triplet.dstAttr._2  // 目标点当前的持股Map
        val ratio: Double = triplet.attr  // 源点对目标点的持股比例

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

    val newGraph2: Graph[(String, Map[VertexId, Double]), Double] = rawGraph.outerJoinVertices(capitalOfVertex)(
      (_: VertexId, vdata: (String, Map[VertexId, Double]), nvdata: Option[Map[VertexId, Double]]) => {
        val newVertexDataMap: Map[VertexId, Double] = nvdata.getOrElse(Map(999999999L -> 0.00))
        (vdata._1, vdata._2 ++ newVertexDataMap)  // (名字，Map 合并）
      }
    )

    newGraph2.vertices.foreach(println)
  }
}
