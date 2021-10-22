import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
 * Graph Demo 15 为了语雀记录而使用
 *
 * 究极简化版
 *
 */

object G015_yuqueData {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkLocalConf().sc

    // 默认投资信息
    val defaultVertex = Map(99999L -> Map(88888L -> 0.00))

    /* 创建边关系，边的属性就是投资金额
    * 边的格式： Edge(srcId, dstId, attr)
    */
    val shareEdgeSeq = Seq(
      Edge(1L, 6L, 1.0), // 青毛狮子怪 -> 狮驼岭集团
      Edge(6L, 4L, 1.0), // 狮驼岭集团 -> 右护法
      Edge(6L, 3L, 1.0), // 狮驼岭集团 -> 左护法
      Edge(4L, 5L, 0.675325), // 右护法 -> 小钻风
      Edge(3L, 5L, 0.324675) // 左护法 -> 小钻风
    )
    val shareEdgeRDD: RDD[Edge[Double]] = sc.parallelize(shareEdgeSeq)
    // 构建初始图
    val rawGraph = Graph.fromEdges(shareEdgeRDD, defaultVertex)

    val initVertex = rawGraph.aggregateMessages[Map[VertexId, Map[VertexId, Double]]](
      triplet => {
        val ratio: Double = triplet.attr  // 持股比例
        val dstId: VertexId = triplet.dstId  // 目标ID
        val vData = Map(dstId -> Map(dstId -> ratio))

        triplet.sendToSrc(vData)
      },
      // Merge Message
      (left: Map[VertexId, Map[VertexId, Double]], right: Map[VertexId, Map[VertexId, Double]]) => {
        left ++ right
      }
    )

    initVertex.collect.foreach(println)
  }
}
