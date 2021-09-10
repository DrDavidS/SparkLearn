import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Graph Demo第二步：各种点和边的计算
 *
 * 我们假定腾讯的投资人就只有这四个自然人，那么腾讯的总注册金额是多少呢？
 * 这里尝试对四个投资人的投资金额进行求和。也就是对顶点 5L 的入边属性求和
 *
 *
 *
 */

object G02_Counting {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("GraphX Ops")
    val sc = new SparkContext(sparkConf)

    // 创建顶点，包括自然人和法人
    val vertexSeq = Seq(
      (1L, ("马化腾", "自然人")),
      (2L, ("陈一丹", "自然人")),
      (3L, ("许晨晔", "自然人")),
      (4L, ("张志东", "自然人")),
      (5L, ("深圳市腾讯计算机系统有限公司", "法人")),
      (6L, ("武汉鲨鱼网络直播技术有限公司", "法人")),
      (7L, ("武汉斗鱼网络科技有限公司", "法人"))
    )
    val vertexSeqRDD: RDD[(VertexId, (String, String))] = sc.parallelize(vertexSeq)

    // 创建边关系，边的属性就是投资金额
    val shareEdgeSeq = Seq(
      Edge(1L, 5L, 3528.6),
      Edge(2L, 5L, 742.9),
      Edge(3L, 5L, 742.9),
      Edge(4L, 5L, 1485.7),
      Edge(5L, 6L, 50.0),
      Edge(7L, 6L, 50.0)
    )
    val shareEdgeRDD: RDD[Edge[Double]] = sc.parallelize(shareEdgeSeq)

    // 构建初始图
    val graph: Graph[(String, String), Double] = Graph(vertexSeqRDD, shareEdgeRDD)

    /*
     * STEP2
     * 我们应该怎么计算腾讯的总注册资金？
     * 1. 首先找到投资腾讯公司的边
     * 2. 将这些边属性聚合
     *
     * 还有一种似乎更简单的思路，直接计算腾讯公司的入度边属性之和。
     */


    // 直接投资腾讯的边
    val shareEdgeInvTencent: RDD[Edge[Double]] = graph.edges.filter((e: Edge[Double]) => e.dstId == 5L)
    shareEdgeInvTencent.collect.foreach(println)
    // 聚合边的属性
  }
}
