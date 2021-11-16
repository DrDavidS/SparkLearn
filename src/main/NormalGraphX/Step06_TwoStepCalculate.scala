import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
 * Step06 股权图谱的多步计算-两步
 *
 * 之前我们都只计算了单步持股关系，现在我们希望计算一下多步持股。比如老板持有子公司股份，子公司又持有孙公司
 * 的股份，问老板对孙公司的股份持有占比多少？
 *
 * 这里为了计算方便，我们重新建立一张图，图的数据采用了西游记里面一些地名和人物。
 *
 * 图的边直接用持股比例代替。不然从一开始的金额去计算比例，代码会显得非常冗长。
 *
 * */

object Step06_TwoStepCalculate {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkLocalConf().sc

    val vertexSeq = Seq(
      (1L, ("青毛狮子怪", Map(6L -> 1.00))),
      (2L, ("金翅大鹏雕", Map(3L -> 0.50))),
      (3L, ("左护法", Map(5L -> 0.324675, 7L -> 1.00))),
      (4L, ("右护法", Map(5L -> 0.675325))),
      (5L, ("小钻风", Map(999999999L -> 0.00))),
      (6L, ("狮驼岭集团", Map(3L -> 1.00))),
      (7L, ("总钻风", Map(999999999L -> 0.00)))
    )
    val vertexSeqRDD: RDD[(VertexId, (String, Map[VertexId, Double]))] = sc.parallelize(vertexSeq)

    /* 创建边关系，边的属性就是投资金额
    * 边的格式： Edge(源ID, 目标ID, 边属性)
    */
    val shareEdgeSeq = Seq(
      Edge(1L, 6L, 1.00), // 青毛狮子怪 -> 狮驼岭集团
      Edge(2L, 3L, 0.50), // 青毛狮子怪 -> 狮驼岭集团
      Edge(6L, 3L, 0.50), // 狮驼岭集团 -> 左护法
      Edge(4L, 5L, 0.675325), // 右护法 -> 小钻风
      Edge(3L, 5L, 0.324675), // 左护法 -> 小钻风
      Edge(3L, 7L, 1.00) // 左护法 -> 总钻风
    )
    val shareEdgeRDD: RDD[Edge[Double]] = sc.parallelize(shareEdgeSeq)

    // 构建初始图
    val rawGraph: Graph[(String, Map[VertexId, Double]), Double] = Graph(vertexSeqRDD, shareEdgeRDD)

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
      _ ++ _
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
