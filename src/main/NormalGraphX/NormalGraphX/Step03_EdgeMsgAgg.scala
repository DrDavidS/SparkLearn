package NormalGraphX

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, EdgeContext, Graph, VertexRDD}
import org.apache.spark.rdd.RDD

/**
 * Step03 在图上进行简单的计算
 *
 * 在创建完毕以后，我们会面临正式应用的第一个问题。如何通过已知的源对目标的投资资本，去计算源对目标的投资比例？
 *
 * 这里我们会 aggregateMessages 这个方法，这是一个非常重要的方法，几乎贯穿始终。
 *
 * */

object Step03_EdgeMsgAgg {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkLocalConf().sc

    val defaultVertex = 0.00
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
    val rawGraph: Graph[Double, Double] = Graph.fromEdges(shareEdgeRDD, defaultVertex)

    /* 发送并且聚合消息 —— aggregateMessages
     *
     * Graph 的 aggregateMessages 分为两大步骤：
     * 首先是发送消息：在下面的代码中我们可以看到，发送消息是针对三元组 triplet 的操作。
     * triplet 既然叫三元组，那么自然是由两点一线构成的。这两点就是 源点src 和 目标点dst，一边自然是三元组本身的边
     *
     * 思考：既然要计算每个股东对公司的投资比例，首先要计算公司的资本总额，再拿每个股东投资的钱除以总额才行。
     * 那么如何计算总额呢？我们需要对每个公司的 入边 数据相加。
     * 那首先我们要将所有入边的数据汇总，这就是代码中的 triplet.sendToDst(money) 意义所在。
     *
     * 在完成汇总以后，所有的入边数据，也就是投资金额已经汇集到了dst顶点，现在需要将他们求和。
     * 直接 _ + _ 即可，和 Spark 编程的 reduce 如出一辙。
     *
     * 注意，这里我们返回的数据类型是 VertexRDD[Double] 而不是一个 Graph
     * 这个 VertexRDD[Double] 中保存的是各个顶点入边上金额求和的结果，只有顶点信息，没有边信息
     */
    val CapitalOfVertex: VertexRDD[Double] = rawGraph.aggregateMessages[Double](
      (triplet: EdgeContext[Double, Double, Double]) => {
        val money: Double = triplet.attr // 💰的数量
        triplet.sendToDst(money)
      },
      // Merge Message
      _ + _
    )

    // 备注：Double可能会有精度问题，可以转而使用BigDecimal
    CapitalOfVertex.collect.foreach(println)
  }
}
