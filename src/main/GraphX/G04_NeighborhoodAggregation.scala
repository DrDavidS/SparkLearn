import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{EdgeContext, Graph, VertexId, VertexRDD}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 领域聚合
 *
 * 许多图分析任务中的一个关键步骤是:聚合有关每个顶点邻域的信息。
 * 例如，我们可能想知道每个用户的关注者数量或每个用户的关注者的平均年龄。
 * 许多迭代图算法（例如，PageRank、最短路径和连通分量）重复聚合相邻顶点的属性
 * （例如，当前 PageRank 值、到源的最短路径和最小可达顶点 id）。
 *
 */

object G04_NeighborhoodAggregation {
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

    // 计算老一些的关注用户的总年龄
    val olderFollowers: VertexRDD[(Int, Double)] = graph.aggregateMessages[(Int, Double)]( // 聚合函数
      triplet => { // 做一个map操作
        if (triplet.srcAttr > triplet.dstAttr) { // 如果源的"Attr"大于目标的，注意这里是一个follow关系
          // Send message to destination vertex containing counter and age
          // 将消息发送给目标顶点，包括计数和年龄
          triplet.sendToDst((1, triplet.srcAttr))
        }
      },
      // 把计数器和年龄加起来
      // a._1 是计数
      // a._2 是年龄
      (a: (Int, Double), b: (Int, Double)) => (a._1 + b._1, a._2 + b._2) // Reduce Function
    )
    // Divide total age by number of older followers to get average age of older followers
    // 返回大龄用户的( 顶点id 和 大龄用户的平均年龄)
    val avgAgeOfOlderFollowers: VertexRDD[Double] =
    olderFollowers.mapValues((id: VertexId, value: (Int, Double)) =>
      value match {
        case (count, totalAge) => totalAge / count
      }
    )
    // Display the results
    avgAgeOfOlderFollowers.collect.foreach(println)
  }
}
