package NormalGraphX

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Step02 创建一个简单的图谱，只使用边信息
 *
 * 我们以公开信息创建一个"深圳市腾讯计算机系统有限公司"的股权图谱（子图）
 *
 * 这里我们只采用 边属性shareEdgeSeq 创建一个简单的图谱
 *
 * 最后我们可以打印出这个图谱的三元组、顶点、边
 *
 * */

object Step02_BuildGraphWithEdge {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkLocalConf().sc

    /* 默认投资信息
     * 这个默认投资信息是干啥的呢？就是给每个顶点一个默认信息（顶点的默认属性）
     * 这里为了不影响计算，我们给 0.00就行
     */
    val defaultVertex = 0.00

    /* 创建边关系，边的属性就是投资金额
    * 边的格式： Edge(源ID, 目标ID, 边属性)
    *
    * 其中斗鱼这块认缴资金是虚构的
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
    val graph: Graph[Double, Double] = Graph.fromEdges(shareEdgeRDD, defaultVertex)

    // 打印点和边
    // 注意打印出来的顶点属性，都是之前塞进去的默认属性 defaultVertex
    println("\n================ 打印顶点 ===================")
    graph.vertices.collect.foreach(println)
    println("\n================ 打印边 ===================")
    graph.edges.foreach(println)
  }
}
