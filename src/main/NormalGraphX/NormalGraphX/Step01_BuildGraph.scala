package NormalGraphX

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}
import org.apache.spark.rdd.RDD

/**
 * Step01 创建一个简单的图谱
 *
 * 我们以公开信息创建一个"深圳市腾讯计算机系统有限公司"的股权图谱（子图）
 * 这大概是最简单的股权图谱了。
 *
 * 我们利用 顶点属性vertexSeq 和 边属性shareEdgeSeq 创建了一个简单的图谱
 *
 * 最后我们可以打印出这个图谱的三元组、顶点、边
 *
 * */

object Step01_BuildGraph {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkLocalConf().sc

    // 创建顶点，包括自然人和法人
    val vertexSeq = Seq(
      (1L, ("马化腾", "自然人")),
      (2L, ("陈一丹", "自然人")),
      (3L, ("许晨晔", "自然人")),
      (4L, ("张志东", "自然人")),
      (5L, ("深圳市腾讯计算机系统有限公司", "法人")),
      (6L, ("武汉鲨鱼网络直播技术有限公司", "法人")),
      (7L, ("武汉斗鱼网络科技有限公司", "法人")),
      (8L, ("张文明", "自然人")),
      (9L, ("陈少杰", "自然人")),
      (10L, ("深圳市鲨鱼文化科技有限公司", "法人"))
    )
    val vertexSeqRDD: RDD[(VertexId, (String, String))] = sc.parallelize(vertexSeq)

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
    val graph: Graph[(String, String), Double] = Graph(vertexSeqRDD, shareEdgeRDD)

    println("\n================ 打印投资三元组关系 ===================")
    graph.triplets.map(
      (triplet: EdgeTriplet[(String, String), Double]) =>
        s"${triplet.srcAttr._1}，${triplet.srcAttr._2}，投资了 ${triplet.dstAttr._1} ，其认缴金额为 ${triplet.attr} 万元"
    ).collect.foreach(println)

    // 打印点和边
    println("\n================ 打印顶点 ===================")
    graph.vertices.collect.foreach(println)
    println("\n================ 打印边 ===================")
    graph.edges.foreach(println)
  }
}
