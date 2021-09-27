import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

/**
 * Graph Demo 第⑦步：真正计算两步控股关系
 *
 */

object G07_CrossShareHolding_v3 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkLocalConf().sc

    // 定义默认信息，以防止某些边与未知用户出现关系
    // 或者供 getOrElse 使用
    val defaultInvestmentInfo = Map(99999L -> investmentInfo())
    val defaultVertex: baseProperties = baseProperties("default_com_name", 0.0, defaultInvestmentInfo)

    // 创建顶点，包括自然人和法人
    val vertexSeq = Seq(
      (1L, baseProperties("马化腾",  0.0, defaultInvestmentInfo)),
      (2L, baseProperties("陈一丹", 0.0, defaultInvestmentInfo)),
      (3L, baseProperties("许晨晔",  0.0, defaultInvestmentInfo)),
      (4L, baseProperties("张志东",  0.0, defaultInvestmentInfo)),
      (5L, baseProperties("深圳市腾讯计算机系统有限公司",  0.0, defaultInvestmentInfo)),
      (6L, baseProperties("武汉鲨鱼网络直播技术有限公司",  0.0, defaultInvestmentInfo)),
      (7L, baseProperties("武汉斗鱼网络科技有限公司",  0.0, defaultInvestmentInfo)),
      (8L, baseProperties("张文明",  0.0, defaultInvestmentInfo)),
      (9L, baseProperties("陈少杰",  0.0, defaultInvestmentInfo)),
      (10L, baseProperties("深圳市鲨鱼文化科技有限公司",  0.0, defaultInvestmentInfo)),
      (11L, baseProperties("成都霜思文化传播有限公司",  0.0, defaultInvestmentInfo))
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
    val graph: Graph[baseProperties, Double] = Graph(vertexSeqRDD, shareEdgeRDD, defaultVertex)

    /*
     * STEP1 第一次 aggregateMessages
     * 先聚合起来，得到每个公司的总注册资金
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
        baseProperties(vd.name, sumOfMoney, vd.oneStepInvInfo)
        // 名称、类型、年龄、总注册资本、Map(一阶投资信息)
      }
    )
    // 新建一张图
    val newGraph: Graph[baseProperties, Double] = Graph(newVertexWithMoney, graph.edges, defaultVertex)

    /*
     * STEP3 第二次aggregateMessages
     * 总资金除以边上资金，得到资金占比，返回src
     */

    val proportionOfShareHolding: VertexRDD[Map[VertexId, investmentInfo]] = newGraph.aggregateMessages[Map[VertexId, investmentInfo]](
      (triplet: EdgeContext[baseProperties, Double, Map[VertexId, investmentInfo]]) => {
        val oneInvestmentMoney: BigDecimal = BigDecimal(triplet.attr) // 单个股东投资资金，此信息在边上面
        val totalInvestment: BigDecimal = triplet.dstAttr.registeredCapital // 企业总注册资本
        val investedCompanyId: VertexId = triplet.dstId // 被投资企业id
        val investedComName: String = triplet.dstAttr.name // 被投资企业名称
        val upperStream: VertexId = triplet.srcId //股东id


        val directSharePercentage: String = (oneInvestmentMoney / totalInvestment).formatted("%.6f")

        // 这里传一个hashmap，其key是公司名称，value是 investmentInfo类，里面有各种信息
        val investmentMap = Map(investedCompanyId ->
          investmentInfo(
            investedComName // 被投资企业名称
            , directSharePercentage // 投资占比
            , totalInvestment // 注册资本
            , upperStream // 上游股东id
          ))
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
        val mapOfInvProportion: Map[VertexId, investmentInfo] = nvd.getOrElse(defaultInvestmentInfo) // 默认属性
        baseProperties(vd.name, vd.registeredCapital, mapOfInvProportion)
        // 名称、类型、年龄【自然人】、总注册资本【法人】、投资占比
      }
    )
    // 新建一张图 newGraph2
    val SecondNewGraph: Graph[baseProperties, Double] = Graph(newVertexWithInvInfo, graph.edges, defaultVertex)
    //    println("\n================ 打印newGraph2新生成的顶点 ===================\n")
    //    SecondNewGraph.vertices.collect.foreach(println)

    /*
     * STEP5 第三次aggregateMessages
     * 计算二阶持股的问题，注意：
     * 1. 这里不考虑环状持股
     * 2. 注意不满足二阶怎么处理 -> 设置默认值
     *
     *  一般来说高精度的计算方法是严格算钱，但是为了简单起见，我们这里保留了持股占比到小数点后6位，
     * 这样直接直接比例相乘会简单一些，精度也能得到保障
     *
     * 首先我们计算
     *
     */
    // TODO 整个加一个 For 循环，限制在3次循环
    // TODO 修改变量名称，当前名称非常不规范
    val nStepOfShareHolding: VertexRDD[Map[VertexId, investmentInfo]] = SecondNewGraph.aggregateMessages[Map[VertexId, investmentInfo]](
      (triplet: EdgeContext[baseProperties, Double, Map[VertexId, investmentInfo]]) => {
        /* 在这里计算多层级，首先举个例子：
         * 以 腾讯 和 武汉鲨鱼 这条边为例子，看看上下游节点存在的信息：
         *
         * srcInvestInfo: Map(6 -> investmentInfo(武汉鲨鱼网络直播技术有限公司,0.500000,50.0,100.0,5,1))
         * dstInvestInfo: Map(10 -> investmentInfo(深圳市鲨鱼文化科技有限公司,1.000000,500.0,500.0,6,1),
         *                    11 -> investmentInfo(成都霜思文化传播有限公司,1.000000,934.1888,934.1888,6,1))
         *
         * 从上面可以看出， 武汉鲨鱼 这个点中汇集了它本身对"霜思文化"和"深圳鲨鱼"的投资信息。
         * 自然而然地，我们可以想到，需要对下游Map做一次遍历，把下游的Map中对更下游投资的信息，
         * 分别和上游（腾讯）对武汉鲨鱼的投资比例相乘，
         * 从而得到腾讯对更下游投资的比例。
         *
         * 备注：实际上上游也可能有多个投资信息，这里我们采用O(n)的方法，不提高复杂度。
         *
         * 按照上面的逻辑，我们将相关步骤写出：
         *
         * 0. 将三元组拆分，其中边上游投资信息Map存入 srcInvestInfo 中
         *    边下游投资信息Map存入 dstInvestInfo 中
         *
         * 1. 遍历，foreach dstInvestInfo：
         *     1.1 当前 kv 对中的 key 就是被边下游顶点投资的企业的ID（可以认为是更下游），存入 dstInvestComID
         *
         *     1.2 v 的 investedComName 参数就是 dstInvestComID 对应的公司名称，存入 dstInvestComName
         *
         *     1.3 v 的 totalInvestment 参数就是 dstInvestComID 对应的公司的注册资本，存入 dstInvestComRegisteredCapital
         *
         *      1.4 dstInvestComID 对应的持股方的ID(kv._2.upperStreamId)作为key，去边上游投资信息Map，
         *          即 srcInvestInfo 中寻找对应的 value。注意这里采用 getOrElse，防止找不到。
         *
         *      1.5 备注：这里我们采用了高精度的持股比例，可以直接用持股比例连乘。
         *          获取当前 srcLinkDstInfo 中的持股比例，存入 srcProportionOfInvestment
         *          获取当前边下游顶点对 dstInvestComID 投资比例 kv._2.proportionOfInvestment
         *          存入 dstProportionOfInvestment
         *
         *      1.6 两个比例相乘，得到边上游对当前循环到 dstInvestComID 的控制比例，存入 mulLevelProportionOfInvestment
         *          备注：这个方法不仅可以计算二阶，还可以计算多阶，而且从原理上是不需要额外代码的
         *
         *      1.7 将多级持股比例的计算结果组合成 Map 放回 investmentMap
         *
         * 2. 发送，这里是 Map 的合并，只需要 _ ++ _ 就行了
         *
         */
        val srcInvestInfo: Map[VertexId, investmentInfo] = triplet.srcAttr.oneStepInvInfo
        val dstInvestInfo: Map[VertexId, investmentInfo] = triplet.dstAttr.oneStepInvInfo

        dstInvestInfo.foreach((kv: (VertexId, investmentInfo)) => {
          // dstInvestInfo 的上游id，去srcInvestInfo里面查询

          // 当前循环到的id, 就是深圳鲨鱼的ID
          val dstInvestComID: VertexId = kv._1
          // 当前循环到的名称, 深圳市鲨鱼文化科技有限公司
          val dstInvestComName: String = kv._2.investedComName
          // 深圳鲨鱼的注册资本
          val dstInvestComRegisteredCapital: BigDecimal = kv._2.registeredCapital
          // 腾讯的ID
          val srcID: VertexId = triplet.srcId


          // 比如 srcLinkDstInfo 的值是 investmentInfo(武汉鲨鱼网络直播技术有限公司,0.500000,50.0,100.0,5,1)
          // 就是腾讯对武汉鲨鱼的投资信息
          val srcLinkDstInfo: investmentInfo = srcInvestInfo.getOrElse(kv._2.upperStreamId, investmentInfo())

          // 获取 腾讯对武汉鲨鱼 的 持股比例
          // srcProportionOfInvestment 的值为 0.500000
          val srcProportionOfInvestment: BigDecimal = BigDecimal(srcLinkDstInfo.proportionOfInvestment)
          // 获取 当前循环到的 dstInvestInfo ，即武汉鲨鱼对深圳鲨鱼的比例, 1.000000
          val dstProportionOfInvestment: BigDecimal = BigDecimal(kv._2.proportionOfInvestment)
          // 相乘，得到两步结果，0.500000
          val mulLevelProportionOfInvestment: String = (srcProportionOfInvestment * dstProportionOfInvestment).formatted("%.6f")
          // 放回Map
          val investmentMap = Map(dstInvestComID ->
            investmentInfo(
              investedComName = dstInvestComName // 被投资企业名称
              , proportionOfInvestment = mulLevelProportionOfInvestment // 投资占比
              , registeredCapital = dstInvestComRegisteredCapital // 总注册资本
              , upperStreamId = srcID // 上游股东id
              // , level = 1
            ))
          // 当前只有多级的投资Map，需要和旧Map合并起来
          val newUnionOldInvestmentMap: Map[VertexId, investmentInfo] = srcInvestInfo ++ investmentMap
          // TODO: 当前层级还是默认的1，应该改为2
          triplet.sendToSrc(newUnionOldInvestmentMap)
        }
        )
      },
      _ ++ _
      // 在这里发送
    )

    // TODO: 合并生成新图
    /*
     * STEP6 又来一次Join，把二级投资的 Map 弄进去
     *
     * 新建一张图 newGraph3，此图将会合并二级股权穿透的信息
     * 就是拿 newGraph2 去 leftJoin nStepOfShareHolding
     *
     */

    val newVertexWithMulLevelInvestInfo: VertexRDD[baseProperties] = SecondNewGraph.vertices.leftZipJoin(nStepOfShareHolding)(
      (vid: VertexId, vd: baseProperties, nvd: Option[Map[VertexId, investmentInfo]]) => {
        val mapOfInvProportion: Map[VertexId, investmentInfo] = nvd.getOrElse(defaultInvestmentInfo) // 默认属性
        baseProperties(
          vd.name, // 姓名
          vd.registeredCapital, // 注册资本
          mapOfInvProportion) // 持股信息
      }
    )
    // 新建一张图 thirdNewGraph
    val thirdNewGraph: Graph[baseProperties, Double] = Graph(newVertexWithMulLevelInvestInfo, graph.edges, defaultVertex)
    println("\n================ 打印 thirdNewGraph 新生成的顶点 ===================\n")
    thirdNewGraph.vertices.collect.foreach(println)
  }
}
