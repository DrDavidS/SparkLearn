import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec
import scala.collection.immutable

/**
 * Graph Demo 09 多边持股判断
 *
 * 重制DAG
 *
 */

object G10_RC_ShareHolding_DAG {
  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    val sc: SparkContext = SparkLocalConf().sc

    // 定义默认信息，以防止某些边与未知用户出现关系
    // 或者供 getOrElse 使用
    val defaultInvestmentInfo = Map(99999L -> investmentInfo())
    val defaultVertex: baseProperties = baseProperties("default_com_name", 0.0, defaultInvestmentInfo)

    // 创建顶点，包括自然人和法人
    val vertexSeq = Seq(
      (1L, baseProperties("青毛狮子怪", 0.0, defaultInvestmentInfo)),
      (3L, baseProperties("狮驼岭左护法有限公司", 0.0, defaultInvestmentInfo)),
      (4L, baseProperties("狮驼岭右护法有限公司", 0.0, defaultInvestmentInfo)),
      (5L, baseProperties("狮驼岭小钻风巡山有限公司", 0.0, defaultInvestmentInfo)),
      (6L, baseProperties("狮驼岭集团股份有限公司", 0.0, defaultInvestmentInfo)),
    )
    val vertexSeqRDD: RDD[(VertexId, baseProperties)] = sc.parallelize(vertexSeq)

    /* 创建边关系，边的属性就是投资金额
    * 边的格式： Edge(srcId, dstId, attr)
    *
    * 其中斗鱼这块认缴资金是虚构的
    */
    val shareEdgeSeq = Seq(
      Edge(1L, 6L, 2000.0), // 青毛狮子怪 -> 狮驼岭集团
      Edge(6L, 4L, 125.0), // 狮驼岭集团 -> 右护法
      Edge(6L, 3L, 177.0), // 狮驼岭集团 -> 左护法
      Edge(4L, 5L, 520.0), // 右护法 -> 小钻风
      Edge(3L, 5L, 250.0), // 左护法 -> 小钻风
    )
    val shareEdgeRDD: RDD[Edge[Double]] = sc.parallelize(shareEdgeSeq)

    // 构建初始图
    val graph: Graph[baseProperties, Double] = Graph(vertexSeqRDD, shareEdgeRDD, defaultVertex)

    /*
     * STEP1 求企业的总注册资本
     *
     * 当前我们企业是没有总注册资本的，所以需要将顶点的所有入边上的投资金额加起来，得到企业的总注册资本
     * 也就是企业的各个上级股东的总共投资金额
     *
     * 这里是第一次聚合，本次聚合不计算持股比例，只计算总额。
     *
     */

    val sumMoneyOfCompany: VertexRDD[BigDecimal] = graph.aggregateMessages[BigDecimal](
      (triplet: EdgeContext[baseProperties, Double, BigDecimal]) => {
        val money: BigDecimal = triplet.attr
        triplet.sendToDst(money)
      },
      _ + _
    )

    val newVertexWithRegisteredCapital: VertexRDD[baseProperties] = graph.vertices.leftZipJoin(sumMoneyOfCompany)(
      (vid: VertexId, vd: baseProperties, nvd: Option[BigDecimal]) => {
        val sumOfMoney: BigDecimal = nvd.getOrElse(BigDecimal(0.0))
        baseProperties(vd.name, sumOfMoney, vd.oneStepInvInfo)
        // 名称、总注册资本、Map(一阶投资信息)
      }
    )
    // 新建图一：此图额外记录了所有顶点的注册资本（自然人除外）
    val GraphOfRegisteredCapital: Graph[baseProperties, Double] = Graph(newVertexWithRegisteredCapital, graph.edges, defaultVertex)

    /*
     * =================================
     * STEP3 第二次aggregateMessages
     * 总资金除以边上资金，得到资金占比，返回src
     * =================================
     */

    val proportionOfShareHolding: VertexRDD[Map[VertexId, investmentInfo]] = GraphOfRegisteredCapital.aggregateMessages[Map[VertexId, investmentInfo]](
      (triplet: EdgeContext[baseProperties, Double, Map[VertexId, investmentInfo]]) => {
        val oneInvestmentMoney: BigDecimal = BigDecimal(triplet.attr) // 边上游的股东对边下游企业的投资资金
        val registeredCapital: BigDecimal = triplet.dstAttr.registeredCapital // 边下游被投资的企业总注册资本
        val investedCompanyId: VertexId = triplet.dstId // 边下游被投资的企业id
        val investedComName: String = triplet.dstAttr.name // 边下游被投资的企业名称
        val upperStreamShareHolderId: VertexId = triplet.srcId //边上游的股东id

        // 直接持股比例 = 投资资金 / 企业总注册资本
        val directInvestedPercentage: String = (oneInvestmentMoney / registeredCapital).formatted("%.6f")

        val investmentMap = Map(investedCompanyId ->
          investmentInfo(
            investedComName // 被投资企业名称
            , directInvestedPercentage // 投资占比
            , registeredCapital // 注册资本
            , upperStreamShareHolderId // 上游股东id
          ))
        triplet.sendToSrc(investmentMap)
      },
      _ ++ _
    )

    /*
     * STEP4 再来一次Join，把Map弄进去
     */

    val newVertexWithInvInfo: VertexRDD[baseProperties] = GraphOfRegisteredCapital.vertices.leftZipJoin(proportionOfShareHolding)(
      (vid: VertexId, vd: baseProperties, nvd: Option[Map[VertexId, investmentInfo]]) => {
        val mapOfInvProportion: Map[VertexId, investmentInfo] = nvd.getOrElse(defaultInvestmentInfo) // 默认属性
        baseProperties(vd.name, vd.registeredCapital, mapOfInvProportion)
        // 名称、类型、年龄【自然人】、总注册资本【法人】、投资占比
      }
    )
    // 新建一张图 oneStepInvInfoGraph
    val oneStepInvInfoGraph: Graph[baseProperties, Double] = Graph(newVertexWithInvInfo, graph.edges, defaultVertex)

    /**
     *
     * @param OldGraph 输入老的Graph，这里特指上面的一阶 Graph
     * @return 返回多层持股关系的新图，这里是一层
     */
    def nStepShareHoldingCalculate(OldGraph: Graph[baseProperties, Double]): Graph[baseProperties, Double] = {
      val nStepOfShareHolding: VertexRDD[Map[VertexId, investmentInfo]] = OldGraph.aggregateMessages[Map[VertexId, investmentInfo]](
        (triplet: EdgeContext[baseProperties, Double, Map[VertexId, investmentInfo]]) => {
          val srcInvestInfo: Map[VertexId, investmentInfo] = triplet.srcAttr.oneStepInvInfo
          val dstInvestInfo: Map[VertexId, investmentInfo] = triplet.dstAttr.oneStepInvInfo

          dstInvestInfo.foreach((kv: (VertexId, investmentInfo)) => {
            // dstInvestInfo 的上游id，去srcInvestInfo里面查询

            // 下游顶点之下被投资企业的ID：5L
            val dstInvestComID: VertexId = kv._1
            // 下游顶点之下被投资企业的名称：狮驼岭小钻风巡山有限公司
            val dstInvestComName: String = kv._2.investedComName
            // 下游顶点下面被投资企业的注册资本：770万
            val dstInvestComRegisteredCapital: BigDecimal = kv._2.registeredCapital
            // 上游顶点企业:6L
            val srcID: VertexId = triplet.srcId
            // 上游顶点对下游顶点的投资信息
            /*
            Map(
                  3 -> investmentInfo(狮驼岭左护法有限公司,1.000000,177.0,6,1),
                  4 -> investmentInfo(狮驼岭右护法有限公司,1.000000,125.0,6,1)
                )
             */
            val srcLinkDstInfo: investmentInfo = srcInvestInfo.getOrElse(kv._2.upperStreamId, investmentInfo())
            // 层级间隔，下游顶点到再下游被投资企业的层级
            // 1
            val dstLevel: Int = kv._2.level

            // 上游顶点对下游顶点的投资比例
            // 狮驼岭 -> 右护法：1.000000
            val srcProportionOfInvestment: BigDecimal = BigDecimal(srcLinkDstInfo.proportionOfInvestment)
            // 下游顶点对下面的公司的投资比例
            // 右护法 -> 小钻风：0.675325
            val dstProportionOfInvestment: BigDecimal = BigDecimal(kv._2.proportionOfInvestment)
            // 相乘，并限制精度。得到上游顶点对下游顶点下面的公司的投资比例
            // 0.675325
            val mulLevelProportionOfInvestment: String = (srcProportionOfInvestment * dstProportionOfInvestment).formatted("%.6f")
            // 计算当前层级，注意上游顶点和下游顶点的层级间隔肯定是1，而下游顶点到再下游被投资企业的层级则大于等于1，这两个东西相加
            val srcLinkDstLevel: Int = dstLevel + 1

            // 放回Map暂存，等待发送
            // 注意这里 investmentMap 的 addSign 需要设置为 true
            val investmentMap = Map(dstInvestComID -> // 5L
              investmentInfo(
                investedComName = dstInvestComName // 被投资企业名称：狮驼岭小钻风巡山有限公司
                , proportionOfInvestment = mulLevelProportionOfInvestment // 投资占比：0.675325
                , registeredCapital = dstInvestComRegisteredCapital // 总注册资本：770万
                , upperStreamId = srcID // 边上游股东id：6L
                , level = srcLinkDstLevel // 层级间隔：2
                , addSign = true // 后面reduce需要合并，这里改为true
              ))
            val newUnionOldInvestmentMap: Map[VertexId, investmentInfo] = srcInvestInfo ++ investmentMap
            triplet.sendToSrc(newUnionOldInvestmentMap)
          }
          )
        },
        // Reduce
        // https://stackoverflow.com/questions/7076128/best-way-to-merge-two-maps-and-sum-the-values-of-same-key
        (leftMap: Map[VertexId, investmentInfo], rightMap: Map[VertexId, investmentInfo]) => {

          val reduceLeftAndRightMap: Map[VertexId, investmentInfo] = leftMap ++ rightMap.map {
            case (k: VertexId, v: investmentInfo) =>
              // 相加标记
              val leftMapAddSign: Boolean = leftMap.getOrElse(k, investmentInfo()).addSign
              val rightMapAddSign: Boolean = v.addSign

              if (rightMapAddSign && leftMapAddSign) {
                val sumOfProportion: BigDecimal = {
                  BigDecimal(v.proportionOfInvestment) + BigDecimal(leftMap.getOrElse(k, investmentInfo()).proportionOfInvestment)
                }

                k -> investmentInfo(
                  investedComName = v.investedComName // 被投资企业名称
                  , proportionOfInvestment = sumOfProportion.formatted("%.6f") // 投资占比求和
                  , registeredCapital = v.registeredCapital // 总注册资本
                  , upperStreamId = v.upperStreamId // 上游股东id
                  , level = v.level // 层级间隔
                )
              }
              else {
                k -> investmentInfo(
                  investedComName = v.investedComName // 被投资企业名称
                  , proportionOfInvestment = v.proportionOfInvestment // 原始投资占比
                  , registeredCapital = v.registeredCapital // 总注册资本
                  , upperStreamId = v.upperStreamId // 上游股东id
                  , level = v.level // 层级间隔
                )
              }
          }

          // 由于上面的 ++ 过程漏了 leftMap 的 addSign 设置，所以需要全部重置一遍false才行
          reduceLeftAndRightMap.map { case (k2, v2) =>
            k2 -> investmentInfo(
              investedComName = v2.investedComName // 被投资企业名称
              , proportionOfInvestment = v2.proportionOfInvestment // 原始投资占比
              , registeredCapital = v2.registeredCapital // 总注册资本
              , upperStreamId = v2.upperStreamId // 上游股东id
              , level = v2.level // 层级间隔
            )
          }
        }
      )

      /*
       * STEP6 又来一次Join，把二级投资的 Map 弄进去
       *
       */

      // 先统统设置一波false

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

      loop(n, oneStepInvInfoGraph) // loop(递归次数, 初始值)
    }

    println("\n================ 打印最终持股计算新生成的顶点 ===================\n")
    val ShareHoldingGraph: Graph[baseProperties, Double] = tailFact(50) // 理论上递归次数增加不影响结果才是对的
    val endTime = System.currentTimeMillis()
    println("G10运行时间： " + (endTime - startTime))
    ShareHoldingGraph.vertices.collect.foreach(println)
  }
}
