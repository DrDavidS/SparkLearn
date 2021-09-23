import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec

/**
 * Graph Demo 在G07的基础上，把图做得更复杂一些
 *
 */

object G08_CrossShareHolding_v4 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkLocalConf().sc

    // 定义默认信息，以防止某些边与未知用户出现关系
    // 或者供 getOrElse 使用
    val defaultInvestmentInfo = Map(99999L -> investmentInfo())
    val defaultVertex: baseProperties = baseProperties("default_com_name", "其他", "999", 0.0, defaultInvestmentInfo)

    // 创建顶点，包括自然人和法人
    val vertexSeq = Seq(
      (1L, baseProperties("马化腾", "自然人", "50", 0.0, defaultInvestmentInfo)),
      (2L, baseProperties("陈一丹", "自然人", "50", 0.0, defaultInvestmentInfo)),
      (3L, baseProperties("许晨晔", "自然人", "52", 0.0, defaultInvestmentInfo)),
      (4L, baseProperties("张志东", "自然人", "49", 0.0, defaultInvestmentInfo)),
      (5L, baseProperties("深圳市腾讯计算机系统有限公司", "法人", "0", 0.0, defaultInvestmentInfo)),
      (6L, baseProperties("武汉鲨鱼网络直播技术有限公司", "法人", "0", 0.0, defaultInvestmentInfo)),
      (7L, baseProperties("武汉斗鱼网络科技有限公司", "法人", "0", 0.0, defaultInvestmentInfo)),
      (8L, baseProperties("张文明", "自然人", "42", 0.0, defaultInvestmentInfo)),
      (9L, baseProperties("陈少杰", "自然人", "39", 0.0, defaultInvestmentInfo)),
      (10L, baseProperties("深圳市鲨鱼文化科技有限公司", "法人", "0", 0.0, defaultInvestmentInfo)),
      (11L, baseProperties("成都霜思文化传播有限公司", "法人", "0", 0.0, defaultInvestmentInfo)),
      (12L, baseProperties("武汉网娱资产管理咨询有限责任公司", "法人", "0", 0.0, defaultInvestmentInfo)),
      (13L, baseProperties("长沙王猴文化传媒有限公司", "法人", "0", 0.0, defaultInvestmentInfo)),
      (14L, baseProperties("段东杰", "自然人", "41", 0.0, defaultInvestmentInfo)),
      (15L, baseProperties("熊智蓁", "自然人", "38", 0.0, defaultInvestmentInfo)),
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
      Edge(6L, 11L, 934.1888), // 武汉鲨鱼 -> 霜思文化
      Edge(7L, 13L, 35.3), // 武汉斗鱼 -> 长沙王猴
      Edge(7L, 12L, 100.0), // 武汉斗鱼 -> 武汉网娱
      Edge(14L, 13L, 140.0), // 段东杰 -> 长沙王猴
      Edge(15L, 13L, 60.0), // 熊智蓁zhēn -> 长沙王猴
    )
    val shareEdgeRDD: RDD[Edge[Double]] = sc.parallelize(shareEdgeSeq)

    // 构建初始图
    val graph: Graph[baseProperties, Double] = Graph(vertexSeqRDD, shareEdgeRDD, defaultVertex)

    /*
     * STEP1 第一次 aggregateMessages
     */

    val sumMoneyOfCompany: VertexRDD[BigDecimal] = graph.aggregateMessages[BigDecimal](
      (triplet: EdgeContext[baseProperties, Double, BigDecimal]) => {
        val money: BigDecimal = triplet.attr
        triplet.sendToDst(money)
      },
      _ + _
    )

    /*
     * STEP2 将聚合起来的数据sumMoneyOfCompany 和 旧 graph 做一次 join
     */

    val newVertexWithMoney: VertexRDD[baseProperties] = graph.vertices.leftZipJoin(sumMoneyOfCompany)(
      (vid: VertexId, vd: baseProperties, nvd: Option[BigDecimal]) => {
        val sumOfMoney: BigDecimal = nvd.getOrElse(BigDecimal(0.0))
        baseProperties(vd.name, vd.invType, vd.age, sumOfMoney, vd.oneStepInvInfo)
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
      _ ++ _
    )

    /*
     * STEP4 再来一次Join，把Map弄进去
     */

    val newVertexWithInvInfo: VertexRDD[baseProperties] = newGraph.vertices.leftZipJoin(proportionOfShareHolding)(
      (vid: VertexId, vd: baseProperties, nvd: Option[Map[VertexId, investmentInfo]]) => {
        val mapOfInvProportion: Map[VertexId, investmentInfo] = nvd.getOrElse(defaultInvestmentInfo) // 默认属性
        baseProperties(vd.name, vd.invType, vd.age, vd.registeredCapital, mapOfInvProportion)
        // 名称、类型、年龄【自然人】、总注册资本【法人】、投资占比
      }
    )
    // 新建一张图 newGraph2
    val SecondNewGraph: Graph[baseProperties, Double] = Graph(newVertexWithInvInfo, graph.edges, defaultVertex)

    // TODO: 简化代码，把上面的内容整合进 nStepShareHoldingCalculate

    /**
     *
     * @param OldGraph 输入老的Graph，比如原始图或者一阶Graph
     * @return 返回多层持股关系的新图，这里是一层
     */
    def nStepShareHoldingCalculate(OldGraph: Graph[baseProperties, Double]): Graph[baseProperties, Double] = {
      val nStepOfShareHolding: VertexRDD[Map[VertexId, investmentInfo]] = OldGraph.aggregateMessages[Map[VertexId, investmentInfo]](
        (triplet: EdgeContext[baseProperties, Double, Map[VertexId, investmentInfo]]) => {
          val srcInvestInfo: Map[VertexId, investmentInfo] = triplet.srcAttr.oneStepInvInfo
          val dstInvestInfo: Map[VertexId, investmentInfo] = triplet.dstAttr.oneStepInvInfo

          dstInvestInfo.foreach((kv: (VertexId, investmentInfo)) => {
            // dstInvestInfo 的上游id，去srcInvestInfo里面查询

            // 下游顶点下面被投资企业的ID
            val dstInvestComID: VertexId = kv._1
            // 下游顶点被投资企业的名称
            val dstInvestComName: String = kv._2.investedComName
            // 下游顶点下面被投资企业的注册资本
            val dstInvestComRegisteredCapital: BigDecimal = kv._2.registeredCapital
            // 上游顶点企业
            val srcID: VertexId = triplet.srcId
            // 上游顶点对下游顶点的投资信息
            val srcLinkDstInfo: investmentInfo = srcInvestInfo.getOrElse(kv._2.upperStreamId, investmentInfo())
            // 层级间隔，下游顶点到再下游被投资企业的层级
            val dstLevel: Int = kv._2.level

            // 上游顶点对下游顶点的投资比例
            val srcProportionOfInvestment: BigDecimal = BigDecimal(srcLinkDstInfo.proportionOfInvestment)
            // 下游顶点对接受其投资的公司的投资比例
            val dstProportionOfInvestment: BigDecimal = BigDecimal(kv._2.proportionOfInvestment)
            // 相乘，并限制精度
            val mulLevelProportionOfInvestment: String = (srcProportionOfInvestment * dstProportionOfInvestment).formatted("%.6f")
            // 计算当前层级，注意上游顶点和下游顶点的层级间隔肯定是1，而下游顶点到再下游被投资企业的层级则大于等于1，这两个东西相加
            val srcLinkDstLevel:Int = dstLevel + 1
            // 放回Map
            val investmentMap = Map(dstInvestComID ->
              investmentInfo(
                investedComName = dstInvestComName // 被投资企业名称
                , proportionOfInvestment = mulLevelProportionOfInvestment // 投资占比
                , registeredCapital = dstInvestComRegisteredCapital // 总注册资本
                , upperStreamId = srcID // 上游股东id
                , level = srcLinkDstLevel  // 层级间隔
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

      /*
       * STEP6 又来一次Join，把二级投资的 Map 弄进去
       *
       */
      val newVertexWithMulLevelInvestInfo: VertexRDD[baseProperties] = OldGraph.vertices.leftZipJoin(nStepOfShareHolding)(
        (vid: VertexId, vd: baseProperties, nvd: Option[Map[VertexId, investmentInfo]]) => {
          val mapOfInvProportion: Map[VertexId, investmentInfo] = nvd.getOrElse(defaultInvestmentInfo) // 默认属性
          baseProperties(
            vd.name, // 姓名
            vd.invType, // 投资方类型——自然人or法人
            vd.age, // 投资人年龄（自然人）
            vd.registeredCapital, // 注册资本
            mapOfInvProportion) // 持股信息
        }
      )
      // 新建一张图 thirdNewGraph
      val nStepNewGraph: Graph[baseProperties, Double] = Graph(newVertexWithMulLevelInvestInfo, graph.edges, defaultVertex)
      nStepNewGraph
    }

    /*
     * STEP5 在这里启用多层调用
     *
     * 尾递归实现
     */
    def tailFact(n: Int): Graph[baseProperties, Double] = {
      /**
       *
       * @param n       递归次数
       * @param currRes 当前结果
       * @return 递归n次后的的Graph
       */
      @tailrec
      def loop(n: Int, currRes: Graph[baseProperties, Double]): Graph[baseProperties, Double] = {
        if (n == 0) return currRes
        loop(n - 1, nStepShareHoldingCalculate(currRes))
      }

      loop(n, SecondNewGraph) // loop(递归次数, 初始值)
    }

    println("\n================ 打印最终持股计算新生成的顶点 ===================\n")
    val ShareHoldingGraph: Graph[baseProperties, Double] = tailFact(6)
    ShareHoldingGraph.vertices.collect.foreach(println)
  }
}
