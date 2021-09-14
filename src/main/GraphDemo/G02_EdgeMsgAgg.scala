import org.apache.spark.graphx.{Edge, EdgeContext, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Graph Demo第二步：边消息的聚合
 *
 * 我们假定腾讯的投资人就只有这四个自然人，那么腾讯的总注册金额是多少呢？
 * 这里尝试对四个投资人的投资金额进行求和。也就是对顶点 5L 的入边属性求和
 *
 *
 * 这里用到了很重要的 aggregateMessages ：
 * 它聚合来自每个顶点的相邻边和顶点的值。
 * 用户提供的 sendMsg 函数在图的每条边上调用，生成 0 个或更多消息，发送到边中的任一顶点。
 * 然后使用 mergeMsg 函数来组合所有发往同一顶点的消息。
 *
 * 参数类型：A 是指要发送到每个顶点的消息类型。比如我们这里统计总金额，实际上就是Double类型
 * 示例：
 * 我们可以使用这个函数来计算每个顶点的入度
 * scala
 * val rawGraph: Graph[_, _] = Graph.textFile("twittergraph")  // 创建图
 * val inDeg: RDD[(VertexId, Int)] =
 * rawGraph.aggregateMessages[Int](ctx => ctx.sendToDst(1), _ + _)
 *
 *
 */

object G02_EdgeMsgAgg {
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
      (7L, ("武汉斗鱼网络科技有限公司", "法人")),
      (8L, ("张文明", "自然人")),
      (9L, ("陈少杰", "自然人")),
      (10L, ("深圳市鲨鱼文化科技有限公司", "法人"))
    )
    val vertexSeqRDD: RDD[(VertexId, (String, String))] = sc.parallelize(vertexSeq)

    /* 创建边关系，边的属性就是投资金额
    * 边的格式： Edge(srcId, dstId, attr)
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


    /*
     * CASE 2
     * 我们应该怎么计算各个公司的总注册资金？
     * 1. 首先找到直接投资公司的边
     * 2. 将这些边属性聚合
     *
     */

    // 采用 aggregateMessages 注意里面的泛型是double，因为这是投资金额
    println("====== CASE 2 ======")
    val invMoney: VertexRDD[Double] = graph.aggregateMessages[Double]((ctx: EdgeContext[(String, String), Double, Double]) => {
      val value: Double = ctx.attr // ctx.attr 指的是边上的属性，这里就是金额，类型是Double
      ctx.sendToDst(value)
    }, _ + _ // 发送到目标节点，相加
    )

    invMoney.collect.foreach((vid: (VertexId, Double)) => {
      println(s"${vid._1}-${vid._2}")
    })

    // CASE 3
    // 直接投资腾讯的边，我们采用filter的形式，过滤一下投资腾讯的边
    println("====== CASE 3 ======")
    val shareEdgeInvTencent: RDD[Edge[Double]] = graph.edges.filter((e: Edge[Double]) => e.dstId == 5L)
    shareEdgeInvTencent.collect.foreach(println)
  }
}
