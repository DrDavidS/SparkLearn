import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Pregel API
 *
 * 尝试了正反PageRank
 *
 */

object G06_Pregel_v2 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("GraphX Ops")
    val sc = new SparkContext(sparkConf)

    /* 包含距离的边数据的图
    用 GraphGenerators 的 logNormalGraph 方法生成了一个正态分布的，具有100个顶点的图
    其中对边的距离属性转换为double类型
     */
    val shareEdgeSeq = Seq(
      Edge(1L, 3L, BigDecimal(0.5)), // 青毛狮子怪 -> 左护法
      Edge(2L, 4L, BigDecimal(0.5)), // 大鹏金翅雕 -> 右护法
      Edge(3L, 4L, BigDecimal(0.5)), // 左护法 -> 右护法
      Edge(4L, 3L, BigDecimal(0.5)), // 右护法 -> 左护法
    )
    val edgeRDD: RDD[Edge[BigDecimal]] = sc.parallelize(shareEdgeSeq)
    val initialGraph: Graph[BigDecimal, BigDecimal] = Graph.fromEdges(edgeRDD, BigDecimal(0.0))
    val reverseGraph = initialGraph.reverse  // 反转整个图

    val ranks: VertexRDD[Double] = reverseGraph.pageRank(0.0001).vertices

    ranks.collect.foreach(println)
  }
}
