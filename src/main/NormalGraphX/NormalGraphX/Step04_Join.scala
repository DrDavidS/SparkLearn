package NormalGraphX

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
 * Step04 在图上进行join操作
 *
 * Step03 中，我们计算了每个公司的总注册资本，保存在了 CapitalOfVertex 之中
 *
 * 但是这还没结束，因为我们的最终目的是将计算出的值保存在图中。
 * 别忘了一开始我们建图的时候，顶点的默认值可是 0.00
 *
 * 在本节我们会使用Join的方法，将 aggregateMessages 得到的结果重新构建一个新的图
 *
 * */

object Step04_Join {
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

    // 发送并且聚合消息
    val capitalOfVertex: VertexRDD[Double] = rawGraph.aggregateMessages[Double](
      (triplet: EdgeContext[Double, Double, Double]) => {
        val money: Double = triplet.attr // 💰的数量
        triplet.sendToDst(money)
      },
      // Merge Message
      _ + _
    )

    // 方法一：
    // 这里对原图中所有顶点使用 leftZipJoin，将新信息放入。
    // 这种方法更灵活，有时候可以比较方便地对所有顶点属性做遍历、Map处理
    val newVertexRDD: VertexRDD[Double] = rawGraph.vertices.leftZipJoin(capitalOfVertex)(
      (_: VertexId, _: Double, nvd: Option[Double]) => {
        nvd.getOrElse(0.00)
      }
    )
    // 然后再根据点和边，新建一张图
    val newGraph1: Graph[Double, Double] = Graph(newVertexRDD, rawGraph.edges)
    println("=========== newGraph1 =============")
    newGraph1.vertices.collect.foreach(println)


    // 方法二：
    // 直接join到原来的 Graph 里面，如果不做什么额外操作，这种方法更简单
    val newGraph2: Graph[Double, Double] = rawGraph.outerJoinVertices(capitalOfVertex)(
      (_: VertexId, _: Double, nvdata: Option[Double]) => nvdata.getOrElse(0.00)
    )
    println("=========== newGraph2 =============")
    newGraph2.vertices.collect.foreach(println)
  }
}
