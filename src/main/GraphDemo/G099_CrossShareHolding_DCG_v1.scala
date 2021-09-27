import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec

/**
 * Graph Demo 在G07的基础上，把图做得更复杂一些
 *
 */

object G099_CrossShareHolding_DCG_v1 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkLocalConf().sc

    // 定义默认信息，以防止某些边与未知用户出现关系
    // 或者供 getOrElse 使用
    val defaultInvestmentInfo = Map(99999L -> investmentInfo())
    val defaultVertex: baseProperties = baseProperties("default_com_name", 0.0, defaultInvestmentInfo)

    // 创建顶点，包括自然人和法人
    val vertexSeq = Seq(
      (1L, baseProperties("青毛狮子怪", 0.0, defaultInvestmentInfo)),
      (2L, baseProperties("大鹏金翅雕", 0.0, defaultInvestmentInfo)),
      (3L, baseProperties("狮驼岭左护法有限公司", 0.0, defaultInvestmentInfo)),
      (4L, baseProperties("狮驼岭右护法有限公司", 0.0, defaultInvestmentInfo)),
      (5L, baseProperties("狮驼岭小钻风巡山有限公司", 0.0, defaultInvestmentInfo))
    )
    val vertexSeqRDD: RDD[(VertexId, baseProperties)] = sc.parallelize(vertexSeq)

    /* 创建边关系，边的属性就是投资金额
    * 边的格式： Edge(srcId, dstId, attr)
    *
    * 其中斗鱼这块认缴资金是虚构的
    */
    val shareEdgeSeq = Seq(
      Edge(1L, 3L, 1000.0), // 青毛狮子怪 -> 左护法
      Edge(2L, 3L, 1000.0), // 大鹏金翅雕 -> 左护法
      Edge(3L, 4L, 500.0), // 左护法 -> 右护法
      Edge(4L, 3L, 500.0), // 右护法 -> 左护法
      Edge(4L, 5L, 250.0), // 右护法 -> 小钻风
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
            val srcLinkDstLevel: Int = dstLevel + 1
            // 放回Map
            val investmentMap = Map(dstInvestComID ->
              investmentInfo(
                investedComName = dstInvestComName // 被投资企业名称
                , proportionOfInvestment = mulLevelProportionOfInvestment // 投资占比
                , registeredCapital = dstInvestComRegisteredCapital // 总注册资本
                , upperStreamId = srcID // 上游股东id
                , level = srcLinkDstLevel // 层级间隔
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
    val ShareHoldingGraph: Graph[baseProperties, Double] = tailFact(6) // 经过测试，递归次数增加不影响结果
    ShareHoldingGraph.vertices.collect.foreach(println)
  }
}
