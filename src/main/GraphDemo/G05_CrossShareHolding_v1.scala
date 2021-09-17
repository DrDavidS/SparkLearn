import org.apache.spark.graphx.{Edge, EdgeContext, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Graph Demo第五步：计算一步控股关系
 *
 */

// case class Properties(var totalMoney: BigDecimal, var xxx: Double)

object G05_CrossShareHolding_v1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("GraphX Ops")
    val sc = new SparkContext(sparkConf)

    // 创建顶点，包括自然人和法人
    val vertexSeq = Seq(
      (1L, ("马化腾", "自然人", "50")),
      (2L, ("陈一丹", "自然人", "50")),
      (3L, ("许晨晔", "自然人", "52")),
      (4L, ("张志东", "自然人", "49")),
      (5L, ("深圳市腾讯计算机系统有限公司", "法人", "0")),
      (6L, ("武汉鲨鱼网络直播技术有限公司", "法人", "0")),
      (7L, ("武汉斗鱼网络科技有限公司", "法人", "0")),
      (8L, ("张文明", "自然人", "42")),
      (9L, ("陈少杰", "自然人", "39")),
      (10L, ("深圳市鲨鱼文化科技有限公司", "法人", "0")),
      (11L, ("成都霜思文化传播有限公司", "法人", "0"))
    )
    val vertexSeqRDD: RDD[(VertexId, (String, String, String))] = sc.parallelize(vertexSeq)

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
      Edge(5L, 6L, 50.0), // 腾讯 -> 武汉鲨鱼
      Edge(7L, 6L, 50.0), // 武汉斗鱼 -> 武汉鲨鱼
      Edge(8L, 7L, 87.5), // 张文明 -> 武汉斗鱼
      Edge(9L, 7L, 1122.2), // 陈少杰 -> 武汉斗鱼
      Edge(6L, 10L, 500.0), // 武汉鲨鱼 -> 深圳鲨鱼
      Edge(6L, 11L, 934.188) // 武汉鲨鱼 -> 霜思文化
    )
    val shareEdgeRDD: RDD[Edge[Double]] = sc.parallelize(shareEdgeSeq)

    // 构建初始图
    val graph: Graph[(String, String, String), Double] = Graph(vertexSeqRDD, shareEdgeRDD)

    /*
     * STEP1 先聚合起来，得到每个公司的总注册资金
     */

    val sumMoneyOfCompany: VertexRDD[BigDecimal] = graph.aggregateMessages[BigDecimal](
      triplet => {
        val money: BigDecimal = triplet.attr
        triplet.sendToDst(money)
      },
      _ + _ // 将各个股东的认缴金额聚合，得到企业的总注册资本
    )

    /*
     * STEP2 将聚合起来的数据sumMoneyOfCompany 和 旧 graph 做一次 join
     * 我们的目的是将企业的总注册资本加入图的公司熟悉中，但是由于图是不能直接修改，所以需要新建一张图
     *
     * 默认会按照 VertexId 来 join，方法可以选择 leftJoin 和 LeftZipJoin 都行
     * join 完成后，现在我们就得到了一个新的 VertexRDD，然后用这个新的 VertexRDD 和 之前的 RDD[Edge[ED]]
     * 来新建一张图
     */

    val newVertexWithMoney: VertexRDD[(String, String, String, BigDecimal)] = graph.vertices.leftZipJoin(sumMoneyOfCompany)(
      (vid: VertexId, vd: (String, String, String), nvd: Option[BigDecimal]) => {
        val sumOfMoney: BigDecimal = nvd.getOrElse(BigDecimal(0.0))
        (vd._1, vd._2, vd._3, sumOfMoney)
        // 名称、类型、年龄、总注册资本
      }
    )

    // 新建一张图
    val newGraph: Graph[(String, String, String, BigDecimal), Double] = Graph(newVertexWithMoney, graph.edges)

    // 打印点和边
    println("\n================ 打印新生成的顶点 ===================")
    newGraph.vertices.collect.foreach(println)

    /*
     * STEP3 总资金除以边上资金，得到资金占比，返回src
     */

    val proportionOfShareHolding = newGraph.aggregateMessages[Map[VertexId, (String, BigDecimal, BigDecimal)]](
      (triplet: EdgeContext[(String, String, String, BigDecimal), Double, Map[VertexId, (String, BigDecimal, BigDecimal)]]) => {
        val oneInvestigationMoney: BigDecimal = BigDecimal(triplet.attr) // 单个股东投资资金
        val totalInvestigation: BigDecimal = triplet.dstAttr._4 // 企业总注册资本
        val investedCompanyId: VertexId = triplet.dstId // 被投资企业id


        val directSharePercentage: String = (oneInvestigationMoney / totalInvestigation).formatted("%.2f")
        // 这里传一个hashmap，其key是公司名称，value是占比
        val investigationMap = Map(investedCompanyId ->
          (directSharePercentage // 投资占比
            , oneInvestigationMoney // 投资金额
            , totalInvestigation)) // 注册资本
        triplet.sendToSrc(investigationMap)
      },
      // 聚合
      // x 都是 Map
      // 企业总注册资本，我们想要在这里生成一个合并的，新的Map
      (x, y) => x ++ y
    )
    println("\n================ 打印顶点合并后的Map ===================\n")
    proportionOfShareHolding.collect.foreach(println)

    /*
     * STEP4 再来一次Join，把Map弄进去
     */

    val newVertexWithInvInfo = newGraph.vertices.leftZipJoin(proportionOfShareHolding)(
      (vid: VertexId, vd, nvd: Option[Map[VertexId, (String, BigDecimal, BigDecimal)]]) => {
        val mapOfInvProportion: Map[VertexId, (String, BigDecimal, BigDecimal)] = nvd.getOrElse(Map(99999L -> ("0.00", 0, 0))) // 设立一个空属性
        (vd._1, vd._2, vd._3, vd._4, mapOfInvProportion)
        // 名称、类型、年龄【自然人】、总注册资本【法人】、投资占比
      }
    )
    // 新建一张图
    val newGraph2 = Graph(newVertexWithInvInfo, graph.edges)
    println("\n================ 打印新生成的顶点 ===================\n")
    newGraph2.vertices.collect.foreach(println)

  }
}
