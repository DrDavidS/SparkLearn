import org.apache.spark.graphx.{EdgeTriplet, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Graph Demo：子图的构建
 *
 * 有时候我们需要构建子图，子图可以有效减少计算的数据量，缩减图的规模
 *
 * 我们用 subgraph 方法来做。子图可以通过点条件或者边条件来限制。
 * 注意边条件的时候，如果没有额外的点条件限制的话，点不会消失，而是变成孤立点。
 *
 *
 *
 *
 */

object G03_SubGraph {
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
     * CASE 1
     * 如果我们只关心跟腾讯和其上游投资人的关系信息，而不关心下游的斗鱼。
     * 那么我们可以构建一个子图 SubGraph
     *
     * 请注意，有意思的是，子图的所有顶点实际上打印出了所有的顶点，但是三元组里面是不体现的
     * 原因是，我们只构建了子图的边，但是顶点没有做限制，所以顶点作为孤立点存在了子图中
     *
     */
    val txSubGraph: Graph[(String, String), Double] = graph.subgraph(
      (epred: EdgeTriplet[(String, String), Double]) => epred.dstAttr._1 == "深圳市腾讯计算机系统有限公司"
    )

    // 打印点和边
    println("\n================ 打印顶点 ===================")
    txSubGraph.vertices.collect.foreach(println)

    println("\n================ 打印子图的投资三元组关系 ===================")
    txSubGraph.triplets.map(
      (triplet: EdgeTriplet[(String, String), Double]) =>
        s"${triplet.srcAttr._1}，${triplet.srcAttr._2}，投资了 ${triplet.dstAttr._1} ，其认缴金额为 ${triplet.attr} 万元"
    ).collect.foreach(println)

    /*
     * 正确做法可能要我们把点和边一起限制
     * 那么怎么限制比较合适呢？
     * 请考虑 filter 方法
     *
     * 现在我们来看看，只剩腾讯边的情况下，计算一下诸多边之和
     * 注意，这里我们将之前的 Double 类型转换为了 BigDecimal 从而避免了误差
     */

    println("\n====== 统计总投资 ======")
    val invMoney: VertexRDD[BigDecimal] = txSubGraph.aggregateMessages[BigDecimal](ctx => {
      val value: BigDecimal = BigDecimal(ctx.attr) // ctx.attr 指的是边上的属性，这里就是金额，类型是Double
      ctx.sendToDst(value)
    }, _ + _) // 发送到目标节点，相加


    invMoney.collect.foreach((vid: (VertexId, BigDecimal)) => {
      println(s"${vid._1}-${vid._2}")
    })
  }
}
