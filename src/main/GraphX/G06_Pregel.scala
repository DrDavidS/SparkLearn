import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Pregel API
 *
 * 使用 Pregel 算子来表达计算，例如单源最短路径。
 * 注意，这里最关键的就是 pregel 函数。
 * 计算原理参考 https://blog.csdn.net/qq_38265137/article/details/80547763
 */

object G06_Pregel {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("GraphX Ops")
    val sc = new SparkContext(sparkConf)

    /* 包含距离的边数据的图
    用 GraphGenerators 的 logNormalGraph 方法生成了一个正态分布的，具有100个顶点的图
    其中对边的距离属性转换为double类型
     */
    val graph: Graph[Long, Double] =
      GraphGenerators.logNormalGraph(sc, numVertices = 100).mapEdges((e: Edge[Int]) => e.attr.toDouble)

    val sourceId: VertexId = 42 // 默认点ID，神奇的42

    // 初始化图，使得除根之外的所有顶点都具有无穷远的距离。
    val initialGraph: Graph[Double, Double] = graph.mapVertices((id: VertexId, _: VertexId) =>
      if (id == sourceId) 0.0 else Double.PositiveInfinity)

    val sssp: Graph[Double, Double] = initialGraph.pregel(Double.PositiveInfinity)(
      // 分析 PREGEL API 的用法
      (id: VertexId, dist: Double, newDist: Double) =>
        math.min(dist, newDist), // 顶点程序
      (triplet: EdgeTriplet[Double, Double]) => { // 发送消息
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message
    )
    println(sssp.vertices.collect.mkString("\n"))
  }
}
