package NormalGraphX

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, EdgeContext, EdgeTriplet, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

/**
 * Step05 计算股东对公司的持股比例
 *
 * 在 Step04 中， 我们计算了每个公司的总注册资本。这里为了方便起见，我们直接将这个结果写入顶点中构图。
 *
 * 有了总注册资本，有了每个股东各自投资的金额，要计算股东们的持股比例就比较简单了：直接各自投资金额除以总注册资本即可。
 * 然后我们将这些信息存入Map，用 triplet.sendToSrc 发送给三元组的股东顶点保存。
 *
 * 实际上这里还有很多遗留问题。比如Map真的能简单地用 ++ 合并吗？
 *
 * 如果两个子公司对同一个孙公司持股，那么 ++ 可能就会出现问题。在 Step06 中我们会对这种情况做出分析并给出解决方案。
 *
 * */

object Step05_StockRatio {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkLocalConf().sc

    // 创建顶点，包括自然人和法人
    val vertexSeq = Seq(
      (1L, ("马化腾", "自然人", 0.00)),
      (2L, ("陈一丹", "自然人", 0.00)),
      (3L, ("许晨晔", "自然人", 0.00)),
      (4L, ("张志东", "自然人", 0.00)),
      (5L, ("深圳市腾讯计算机系统有限公司", "法人", 6500.00)),
      (6L, ("武汉鲨鱼网络直播技术有限公司", "法人", 100.00)),
      (7L, ("武汉斗鱼网络科技有限公司", "法人", 1209.70)),
      (8L, ("张文明", "自然人", 0.00)),
      (9L, ("陈少杰", "自然人", 0.00)),
      (10L, ("深圳市鲨鱼文化科技有限公司", "法人", 500.00))
    )
    val vertexSeqRDD: RDD[(VertexId, (String, String, Double))] = sc.parallelize(vertexSeq)

    /* 创建边关系，边的属性就是投资金额
    * 边的格式： Edge(源ID, 目标ID, 边属性)
    */
    val shareEdgeSeq = Seq(
      Edge(1L, 5L, 3528.6), // 马化腾 -> 腾讯
      Edge(2L, 5L, 742.9), // 陈一丹 -> 腾讯
      Edge(3L, 5L, 742.9), // 许晨晔 -> 腾讯
      Edge(4L, 5L, 1485.7), // 张志东 -> 腾讯
      Edge(5L, 6L, 50.0), // 腾讯 -> 鲨鱼
      Edge(7L, 6L, 50.0), // 斗鱼 -> 鲨鱼
      Edge(8L, 7L, 87.5), // 张文明 -> 斗鱼
      Edge(9L, 7L, 1122.2), // 陈少杰 -> 斗鱼
      Edge(6L, 10L, 500.0) // 鲨鱼 -> 深圳鲨鱼
    )
    val shareEdgeRDD: RDD[Edge[Double]] = sc.parallelize(shareEdgeSeq)

    // 构建初始图
    val rawGraph: Graph[(String, String, Double), Double] = Graph(vertexSeqRDD, shareEdgeRDD)

    // 发送并且聚合消息
    val capitalOfVertex: VertexRDD[Map[VertexId, Double]] = rawGraph.aggregateMessages[Map[VertexId, Double]](
      (triplet: EdgeContext[(String, String, Double), Double, Map[VertexId, Double]]) => {
        val invMoney: Double = triplet.attr // 三元组的边属性，💰的数量
        val dstTuple: (String, String, Double) = triplet.dstAttr // 三元组的目标点属性，注意这里是一个元组
        val registeredCapital: Double = dstTuple._3 // 注册资本，这里分开写便于理解
        val dstId: VertexId = triplet.dstId // 目标点ID

        /* 现在有了总注册资本 registeredCapital
         * 有了股东投资的金额 invMoney
         * 则控股比例 stockRatio 自然就是：invMoney / registeredCapital
         *
         * 我们希望将控股信息保存在一个 Map 里面，发送给股东
         */
        val stockRatio: Double = invMoney / registeredCapital
        val stockHoldingMap = Map(dstId -> stockRatio)
        triplet.sendToSrc(stockHoldingMap)
      },
      // Merge Message
      (_: Map[VertexId, Double]) ++ (_: Map[VertexId, Double])
    )

    // capitalOfVertex.collect.foreach(println)
    // 再依葫芦画瓢，将顶点RDD信息 join 回去
    val oneStepStockGraph: Graph[Map[VertexId, Double], Double] = rawGraph.outerJoinVertices(capitalOfVertex)(
      (_: VertexId, _: (String, String, Double), nvdata: Option[Map[VertexId, Double]]) =>
        nvdata.getOrElse(Map(999999999L -> 0.00)) // 给一个默认信息，方便没有的情况下填充，比如10号顶点
    )
    println("=========== oneStepStockGraph =============")
    oneStepStockGraph.vertices.collect.foreach(println)
  }
}
