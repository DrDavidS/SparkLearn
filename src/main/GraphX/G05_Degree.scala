import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 领域聚合
 *
 * 许多图分析任务中的一个关键步骤是:聚合有关每个顶点邻域的信息。
 * 例如，我们可能想知道每个用户的关注者数量或每个用户的关注者的平均年龄。
 * 许多迭代图算法（例如，PageRank、最短路径和连通分量）重复聚合相邻顶点的属性
 * （例如，当前 PageRank 值、到源的最短路径和最小可达顶点 id）。
 *
 * 这里计算顶点的度。也就是说顶点和其他领域顶点之间的连接（边）的数量叫做度。
 * 具体分为出度、入度。度就是出入一起。
 */

object G05_Degree {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("GraphX Ops")
    val sc = new SparkContext(sparkConf)

    /* 创建一个有"age"属性的顶点
     * 简单起见，这里采用了一个随机图
     * 这里用 logNormalGraph 生成了一个顶点出度为正态分布的图
     */
    val graph: Graph[Double, Int] =
      GraphGenerators.logNormalGraph(sc, numVertices = 100)
        .mapVertices((id: VertexId, _: VertexId) => id.toDouble)

    // Define a reduce operation to compute the highest degree vertex
    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }
    // 计算出度、入度等的最大值节点
    val maxInDegree: (VertexId, Int)  = graph.inDegrees.reduce(max)
    val maxOutDegree: (VertexId, Int) = graph.outDegrees.reduce(max)
    val maxDegrees: (VertexId, Int)   = graph.degrees.reduce(max)

    println("最大入度节点：" + maxInDegree)
    println("最大出度节点：" + maxOutDegree)
    println("最大度节点：" + maxDegrees)
  }
}
