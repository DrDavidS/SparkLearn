import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec

/**
 * Graph Demo 14 有向有环图
 *
 * 究极简化版
 *
 */

object G014_CrossShareHolding_DCG_SimpleData {
  def main(args: Array[String]): Unit = {
    val startTime: Long = System.currentTimeMillis()
    val sc: SparkContext = SparkLocalConf().sc

    // 默认信息
    val defaultInvestmentInfo = Map(99999L -> simpleInvestmentInfo())
    val defaultVertex: simpleProperties = simpleProperties(
      "default_com_name",
      defaultInvestmentInfo,
    )

    // 创建顶点，包括自然人和法人
    val vertexSeq = Seq(
      (1L, simpleProperties("青毛狮子怪", defaultInvestmentInfo)),
      (2L, simpleProperties("大鹏金翅雕", defaultInvestmentInfo)),
      (3L, simpleProperties("3 左护法", defaultInvestmentInfo)),
      (5L, simpleProperties("5 左护法", defaultInvestmentInfo)),
      (6L, simpleProperties("6 左护法", defaultInvestmentInfo)),
      (7L, simpleProperties("7 左护法", defaultInvestmentInfo)),
      (4L, simpleProperties("4 右护法", defaultInvestmentInfo)),
      (8L, simpleProperties("8 右护法", defaultInvestmentInfo)),
      (9L, simpleProperties("9 右护法", defaultInvestmentInfo)),
      (10L, simpleProperties("10 右护法", defaultInvestmentInfo)),
    )
    val vertexSeqRDD: RDD[(VertexId, simpleProperties)] = sc.parallelize(vertexSeq)

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
    val graph: Graph[simpleProperties, Double] = Graph(vertexSeqRDD, shareEdgeRDD, defaultVertex)

    val oneStepGraph: VertexRDD[Map[VertexId, simpleInvestmentInfo]] = graph.aggregateMessages[Map[VertexId, simpleInvestmentInfo]](triplet => {
      val invRatio: BigDecimal = BigDecimal(triplet.attr)
      val dstId: VertexId = triplet.dstId
      val dstName: String = triplet.dstAttr.name

      val oneStepInv = Map(dstId -> simpleInvestmentInfo(dstName, invRatio))

      triplet.sendToSrc(oneStepInv)
    },
      _ ++ _
    )


    println("\n================ 打印最终持股计算新生成的顶点 ===================\n")
    val endTime: Long = System.currentTimeMillis()
    println("\n运行时间： " + (endTime - startTime))
    oneStepGraph.collect.foreach(println)
  }
}
