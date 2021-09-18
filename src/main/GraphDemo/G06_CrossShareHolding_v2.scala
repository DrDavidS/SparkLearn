import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

/**
 * * Graph Demo第六步：计算两步控股关系
 *
 * 以后计算更多步就可以以此类推了。
 * 同时，由于 Tuple 类型的使用不便，我们可以采用 case class 的形式来处理
 *
 *
 *
 */

object G06_CrossShareHolding_v2 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkLocalConf().sc

    // 创建顶点，包括自然人和法人
    val vertexSeq = Seq(
      (1L, baseProperties("马化腾", "自然人", "50", 0.0, Map(99999L -> investmentInfo()))),
      (2L, baseProperties("陈一丹", "自然人", "50", 0.0, Map(99999L -> investmentInfo()))),
      (3L, baseProperties("许晨晔", "自然人", "52", 0.0, Map(99999L -> investmentInfo()))),
      (4L, baseProperties("张志东", "自然人", "49", 0.0, Map(99999L -> investmentInfo()))),
      (5L, baseProperties("深圳市腾讯计算机系统有限公司", "法人", "0", 0.0, Map(99999L -> investmentInfo()))),
      (6L, baseProperties("武汉鲨鱼网络直播技术有限公司", "法人", "0", 0.0, Map(99999L -> investmentInfo()))),
      (7L, baseProperties("武汉斗鱼网络科技有限公司", "法人", "0", 0.0, Map(99999L -> investmentInfo()))),
      (8L, baseProperties("张文明", "自然人", "42", 0.0, Map(99999L -> investmentInfo()))),
      (9L, baseProperties("陈少杰", "自然人", "39", 0.0, Map(99999L -> investmentInfo()))),
      (10L, baseProperties("深圳市鲨鱼文化科技有限公司", "法人", "0", 0.0, Map(99999L -> investmentInfo()))),
      (11L, baseProperties("成都霜思文化传播有限公司", "法人", "0", 0.0, Map(99999L -> investmentInfo())))
    )
    val vertexSeqRDD: RDD[(VertexId, baseProperties)] = sc.parallelize(vertexSeq)

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
      Edge(6L, 11L, 934.1888) // 武汉鲨鱼 -> 霜思文化
    )
    val shareEdgeRDD: RDD[Edge[Double]] = sc.parallelize(shareEdgeSeq)

    // 构建初始图
    val graph: Graph[baseProperties, Double] = Graph(vertexSeqRDD, shareEdgeRDD)

    /*
     * STEP1 先聚合起来，得到每个公司的总注册资金
     */

    val sumMoneyOfCompany: VertexRDD[BigDecimal] = graph.aggregateMessages[BigDecimal](
      (triplet: EdgeContext[baseProperties, Double, BigDecimal]) => {
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

    val newVertexWithMoney: VertexRDD[baseProperties] = graph.vertices.leftZipJoin(sumMoneyOfCompany)(
      (vid: VertexId, vd: baseProperties, nvd: Option[BigDecimal]) => {
        val sumOfMoney: BigDecimal = nvd.getOrElse(BigDecimal(0.0))
        baseProperties(vd.name, vd.invType, vd.age, sumOfMoney, vd.oneStepInvInfo)
        // 名称、类型、年龄、总注册资本、Map(一阶投资信息)
      }
    )
    // 新建一张图
    val newGraph: Graph[baseProperties, Double] = Graph(newVertexWithMoney, graph.edges)

    /*
     * STEP3 总资金除以边上资金，得到资金占比，返回src
     */

    val proportionOfShareHolding: VertexRDD[Map[VertexId, investmentInfo]] = newGraph.aggregateMessages[Map[VertexId, investmentInfo]](
      (triplet: EdgeContext[baseProperties, Double, Map[VertexId, investmentInfo]]) => {
        val oneInvestmentMoney: BigDecimal = BigDecimal(triplet.attr) // 单个股东投资资金，此信息在边上面
        val totalInvestment: BigDecimal = triplet.dstAttr.totalMoney // 企业总注册资本
        val investedCompanyId: VertexId = triplet.dstId // 被投资企业id
        val investedComName: String = triplet.dstAttr.name // 被投资企业名称

        val directSharePercentage: String = (oneInvestmentMoney / totalInvestment).formatted("%.2f")

        // 这里传一个hashmap，其key是公司名称，value是 investmentInfo类，里面有各种信息
        val investmentMap = Map(investedCompanyId ->
          investmentInfo(
            investedComName // 被投资企业名称
            , directSharePercentage // 投资占比
            , oneInvestmentMoney // 投资金额
            , totalInvestment // 注册资本
          )) // 默认层级
        triplet.sendToSrc(investmentMap)
      },
      // 聚合
      // x 都是 Map
      // 企业总注册资本，我们想要在这里生成一个合并的，新的Map
      _ ++ _
    )

    /*
     * STEP4 再来一次Join，把Map弄进去
     */

    val newVertexWithInvInfo: VertexRDD[baseProperties] = newGraph.vertices.leftZipJoin(proportionOfShareHolding)(
      (vid: VertexId, vd: baseProperties, nvd: Option[Map[VertexId, investmentInfo]]) => {
        val mapOfInvProportion: Map[VertexId, investmentInfo] = nvd.getOrElse(Map(99999L -> investmentInfo())) // 设立一个空属性
        baseProperties(vd.name, vd.invType, vd.age, vd.totalMoney, mapOfInvProportion)
        // 名称、类型、年龄【自然人】、总注册资本【法人】、投资占比
      }
    )
    // 新建一张图 newGraph2
    val newGraph2: Graph[baseProperties, Double] = Graph(newVertexWithInvInfo, graph.edges)
    println("\n================ 打印newGraph2新生成的顶点 ===================\n")
    newGraph2.vertices.collect.foreach(println)

  }
}
