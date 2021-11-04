import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
 * Graph Demo 14 有向有环图
 *
 * 究极简化版
 *
 */

object G014_CrossShareHolding_DCG_SimpleData {
  def main(args: Array[String]): Unit = {
    // val startTime: Long = System.currentTimeMillis()
    val sc: SparkContext = SparkLocalConf().sc

    // 默认投资信息
    val defaultVertex = Map(99999L -> Map(88888L -> BigDecimal(0.00000)))

    // 创建顶点，包括自然人和法人
    val vertexSeq = Seq(
      (1L, Map(99999L -> Map(88888L -> BigDecimal(0.00000)))),
      (2L, Map(99999L -> Map(88888L -> BigDecimal(0.00000)))),
      (3L, Map(99999L -> Map(88888L -> BigDecimal(0.00000)))),
      (5L, Map(99999L -> Map(88888L -> BigDecimal(0.00000)))),
      (6L, Map(99999L -> Map(88888L -> BigDecimal(0.00000)))),
      (7L, Map(99999L -> Map(88888L -> BigDecimal(0.00000)))),
      (4L, Map(99999L -> Map(88888L -> BigDecimal(0.00000)))),
      (8L, Map(99999L -> Map(88888L -> BigDecimal(0.00000)))),
      (9L, Map(99999L -> Map(88888L -> BigDecimal(0.00000)))),
      (10L, Map(99999L -> Map(88888L -> BigDecimal(0.00000)))),
    )
    val vertexSeqRDD: RDD[(VertexId, Map[VertexId, Map[VertexId, BigDecimal]])] = sc.parallelize(vertexSeq)

    /* 创建边关系，边的属性就是投资金额
    * 边的格式： Edge(srcId, dstId, attr)
    */
    val shareEdgeSeq = Seq(
      Edge(1L, 3L, 0.5), // 青毛狮子怪 -> 左护法
      Edge(2L, 4L, 0.5), // 大鹏金翅雕 -> 左护法
      Edge(3L, 5L, 1.0),
      Edge(5L, 6L, 1.0),
      Edge(6L, 7L, 1.0),
      Edge(7L, 4L, 0.5),
      Edge(4L, 8L, 1.0),
      Edge(8L, 9L, 1.0),
      Edge(9L, 10L, 1.0),
      Edge(10L, 3L, 0.5),
    )
    val shareEdgeRDD: RDD[Edge[Double]] = sc.parallelize(shareEdgeSeq)
    // 构建初始图
    val initGraph: Graph[Map[VertexId, Map[VertexId, BigDecimal]], Double] = Graph(vertexSeqRDD, shareEdgeRDD, defaultVertex)

    // 将边上的投资信息转移到节点上
    // 如何做
    //     [Map[VertexId1, Map[VertexId2, Double]]]
    //     VertexId1 是 从此点发过来的
    //     VertexId2 是 发送点 对此点的持股比例
    var flagGraph = initGraph
    for (i <- 0 to 10) {
      val nGraph: VertexRDD[Map[VertexId, Map[VertexId, BigDecimal]]] = initGraph.aggregateMessages[Map[VertexId, Map[VertexId, BigDecimal]]](
        triplet => {
          val ratio: Double = triplet.attr // 投资比例
          val dst: Map[VertexId, Map[VertexId, BigDecimal]] = triplet.dstAttr // 目标属性
          val dstId: VertexId = triplet.dstId // 目标id
          val src: Map[VertexId, Map[VertexId, BigDecimal]] = triplet.srcAttr // 源属性

          // 首先把点Map里面的属性合并
          // 将dst中，各下游节点发送的投资信息取出
          val values: Iterable[Map[VertexId, BigDecimal]] = dst.values
          val tuples: Iterable[(VertexId, BigDecimal)] = values.flatMap(_.toSeq)
          val idToTuples: Map[VertexId, Iterable[(VertexId, BigDecimal)]] = tuples.groupBy(_._1) // 每个节点发送过来的所有数据放在可迭代元组里面
          // 将每个ID对应发送过来的持股元组 Iterable[(VertexId, BigDecimal)]
          val idToDecimal: Map[VertexId, BigDecimal] = idToTuples.mapValues(rr => {
            rr.map(r1 => {
              r1._2 * ratio // 之前的持股比例乘以现在的投资比例
            }).sum
          })

          val vertexIdToDecimal: Map[VertexId, BigDecimal] = idToDecimal.map((row: (VertexId, BigDecimal)) => (row._1, row._2))
          triplet.sendToSrc(Map(dstId -> vertexIdToDecimal))
        },
        // `Merge Messages`
        (message1, message2) => {
          message1 ++ message2
        })

      val baseGraph = initGraph.outerJoinVertices(nGraph)((vid, vdata, nvdata) => {
        val ndataMap: Map[VertexId, Map[VertexId, BigDecimal]] = nvdata.getOrElse(Map.empty)

        val resData = vdata.map((row: (VertexId, Map[VertexId, BigDecimal])) => {
          var shareHoldRatioMap: Map[VertexId, BigDecimal] = row._2
          if (ndataMap.contains(row._1)) { // 如果ndata里面有 VertexID，说明这个节点已经发送过数据了，成环
            val originData: Map[VertexId, BigDecimal] = row._2 // 原始数据
            val ndata: Map[VertexId, BigDecimal] = ndataMap(row._1) // 从发送过来的ndata数据里面，去找同Key的数据

            // 增量的持股信息
            // 从ndata里面挨个过滤，挑出originData里面没有的，说明是新来的信息
            val nKeyMaps = ndata.filter(r => {
              !originData.contains(r._1)
            })

            // 存量的(相同)持股信息
            // 从originData里面去挑选ndata里面有的信息
            // 多次循环肯定是有重复发送的，如果信息差异小于0.0001，就直接保留 row
            val reserve: Map[VertexId, BigDecimal] = originData.filter((tuple: (VertexId, BigDecimal)) => {
              ndata.contains(tuple._1)
            }).map((tupleRow: (VertexId, BigDecimal)) => {
              if (Math.abs((tupleRow._2 - ndata(tupleRow._1)).toDouble) < 0.0001) {
                tupleRow
              }
              else {
                (tupleRow._1, tupleRow._2 + ndata(tupleRow._1))
              }
            })

            //存量的（差异的保留）
            val diffNN: Map[VertexId, BigDecimal] = originData.filter(r => {
              !ndata.contains(r._1)
            })
            shareHoldRatioMap = diffNN ++ reserve ++ nKeyMaps
          }
          (row._1, shareHoldRatioMap)
        })
        resData
      })
      baseGraph.vertices.foreach(row => {
        println(row)
      })
      flagGraph = baseGraph
    }
  }
}
