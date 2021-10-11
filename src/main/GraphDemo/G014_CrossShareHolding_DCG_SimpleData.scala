import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec

/**
 * Graph Demo 14 有向有环图
 *
 * 究极简化版
 *
 */

object G014_CrossShareHolding_DCG_SimpleData {
  def main(args: Array[String]): Unit = {
    val startTime: Long = System.currentTimeMillis()
    val sc: SparkContext = SparkLocalConf().sc

    // 默认信息
    val defaultInvestmentInfo = Map(99999L -> simpleInvestmentInfo())
    val defaultVertex: simpleProperties = simpleProperties("default_com_name", defaultInvestmentInfo)

    // 创建顶点，包括自然人和法人
    val vertexSeq = Seq(
      (1L, simpleProperties("青毛狮子怪", defaultInvestmentInfo)),
      (2L, simpleProperties("大鹏金翅雕", defaultInvestmentInfo)),
      (3L, simpleProperties("3 左护法", defaultInvestmentInfo)),
      (5L, simpleProperties("5 左护法", defaultInvestmentInfo)),
      (6L, simpleProperties("6 左护法", defaultInvestmentInfo)),
      (7L, simpleProperties("7 左护法", defaultInvestmentInfo)),
      (4L, simpleProperties("4 右护法", defaultInvestmentInfo)),
      (8L, simpleProperties("8 右护法", defaultInvestmentInfo)),
      (9L, simpleProperties("9 右护法", defaultInvestmentInfo)),
      (10L, simpleProperties("10 右护法", defaultInvestmentInfo)),
    )
    val vertexSeqRDD: RDD[(VertexId, simpleProperties)] = sc.parallelize(vertexSeq)

    /* 创建边关系，边的属性就是投资金额
    * 边的格式： Edge(srcId, dstId, attr)
    *
    * 其中斗鱼这块认缴资金是虚构的
    */
    val shareEdgeSeq = Seq(
      Edge(1L, 3L, 0.5), // 青毛狮子怪 -> 左护法
      Edge(2L, 4L, 0.5), // 大鹏金翅雕 -> 左护法
      Edge(3L, 5L, 1.0),
      Edge(5L, 6L, 1.0),
      Edge(6L, 7L, 1.0),
      Edge(7L, 4L, 0.5),
      Edge(4L, 8L, 1.0),
      Edge(8L, 9L, 1.0),
      Edge(9L, 10L, 1.0),
      Edge(10L, 3L, 0.5),
    )
    val shareEdgeRDD: RDD[Edge[Double]] = sc.parallelize(shareEdgeSeq)

    // 构建初始图
    val graph: Graph[simpleProperties, Double] = Graph(vertexSeqRDD, shareEdgeRDD, defaultVertex)


    /*
     * =================================
     * STEP1 aggregateMessages
     * 汇集src对所有dst的单步投资关系，发送给src
     * 以Map形式 保存在src节点的 simpleInvestmentInfo 中
     * =================================
     */

    val proportionOfShareHolding: VertexRDD[Map[VertexId, simpleInvestmentInfo]] =
      graph.aggregateMessages[Map[VertexId, simpleInvestmentInfo]](
        (triplet: EdgeContext[simpleProperties, Double, Map[VertexId, simpleInvestmentInfo]]) => {
          val dstVertexComId: VertexId = triplet.dstId
          val dstVertexComName: String = triplet.dstAttr.name
          val srcVertexComId: VertexId = triplet.srcId
          // 三元组：持股比例
          val directInvestedPercentage: String = triplet.attr.formatted("%.6f")

          // 这一步好比是把边上的投资信息发送给了src节点
          val investmentMap = Map(dstVertexComId ->
            simpleInvestmentInfo(
              dstVertexComName, // 被投资企业名称
              directInvestedPercentage, // 投资占比
              srcVertexComId, // 上游股东id
            ))
          triplet.sendToSrc(investmentMap)
        },
        _ ++ _ // 信息聚合，由于两点之间同向边只有一条（多条请提前聚合），直接++
      )

    /*
     * =================================
     * STEP2 leftJoin
     * 单步投资关系的保存。
     * 现在每个节点都有更新了一阶投资信息
     * =================================
     */

    val newVertexWithInvInfo: VertexRDD[simpleProperties] = graph.vertices.leftZipJoin(proportionOfShareHolding)(
      (vid: VertexId, vd: simpleProperties, nvd: Option[Map[VertexId, simpleInvestmentInfo]]) => {
        val mapOfInvProportion: Map[VertexId, simpleInvestmentInfo] = nvd.getOrElse(defaultInvestmentInfo) // 默认属性
        simpleProperties(vd.name, mapOfInvProportion) // 名称、投资占比
      }
    )
    // 新建一张图 oneStepInvInfoGraph
    val oneStepInvInfoGraph: Graph[simpleProperties, Double] = Graph(newVertexWithInvInfo, graph.edges, defaultVertex)

    /**
     *
     * @param OldGraph 输入老的Graph，这里特指上面的一阶 Graph
     * @return 返回多层持股关系的新图，这里是一层
     */
    def nStepShareHoldingCalculate(OldGraph: Graph[simpleProperties, Double]): Graph[simpleProperties, Double] = {
      val nStepOfShareHolding: VertexRDD[Map[VertexId, simpleInvestmentInfo]] = OldGraph.aggregateMessages[Map[VertexId, simpleInvestmentInfo]](
        (triplet: EdgeContext[simpleProperties, Double, Map[VertexId, simpleInvestmentInfo]]) => {
          val srcInvestInfo: Map[VertexId, simpleInvestmentInfo] = triplet.srcAttr.oneStepInvInfo
          val dstInvestInfo: Map[VertexId, simpleInvestmentInfo] = triplet.dstAttr.oneStepInvInfo

          dstInvestInfo.map {
            case (k: VertexId, v: simpleInvestmentInfo) =>
              // 下游顶点之下被投资企业的ID
              val dstInvestComID: VertexId = k
              // 下游顶点之下被投资企业的名称
              val dstInvestComName: String = v.investedComName
              // 上游顶点企业 ID
              val srcID: VertexId = triplet.srcId
              // 从下游投资Map的upperStreamId里面，找到对应的，上游顶点对下游顶点（upperStreamId）的投资信息
              val srcLinkDstInfo: simpleInvestmentInfo = srcInvestInfo.getOrElse(v.upperStreamId, simpleInvestmentInfo())
              // 上游顶点对下游顶点的投资比例
              val srcProportionOfInvestment: BigDecimal = BigDecimal(srcLinkDstInfo.proportionOfInvestment)
              // 下游顶点对下面的公司的投资比例（Map中当前的kv对）
              val dstProportionOfInvestment: BigDecimal = BigDecimal(v.proportionOfInvestment)
              // 相乘，并限制精度。得到上游顶点对下游顶点下面的公司的投资比例
              val mulLevelProportionOfInvestment: String = (srcProportionOfInvestment * dstProportionOfInvestment).formatted("%.6f")
              // 计算当前层级，注意上游顶点和下游顶点的层级间隔肯定是1，而下游顶点到再下游被投资企业的层级则大于等于1，这两个东西相加
              val srcLinkDstLevel: Int = v.level + 1

              // 放回Map暂存，等待发送
              // 注意这里 investmentMap 的 addSign 需要设置为 true
              val investmentMap = Map(dstInvestComID -> // 5L
                simpleInvestmentInfo(
                  investedComName = dstInvestComName, // 被投资企业名称
                  proportionOfInvestment = mulLevelProportionOfInvestment, // 投资占比
                  upperStreamId = srcID, // 边上游股东
                  level = srcLinkDstLevel, // 层级间隔
                ))

              // TODO 这里在if条件里面决定是否发消息
              // 考虑改进方法：如果没有成环后只发一次消息？
              if (true) {
                // 成环处理
                triplet.sendToSrc(investmentMap)
              } else {
                println("========== 发现成环 ========")
              }
          }
        },
        // 消息聚合阶段
        // https://stackoverflow.com/questions/7076128/best-way-to-merge-two-maps-and-sum-the-values-of-same-key
        (leftMap: Map[VertexId, simpleInvestmentInfo], rightMap: Map[VertexId, simpleInvestmentInfo]) => {

          val reduceLeftAndRightMap: Map[VertexId, simpleInvestmentInfo] = leftMap ++ rightMap.map {
            case (k: VertexId, v: simpleInvestmentInfo) =>
              // 多个下游传来的，对同一个子孙企业的投资比例相加
              val sumOfProportion: BigDecimal = {
                BigDecimal(v.proportionOfInvestment) + BigDecimal(leftMap.getOrElse(k, simpleInvestmentInfo()).proportionOfInvestment)
              }
              k -> simpleInvestmentInfo(
                investedComName = v.investedComName, // 被投资企业名称
                proportionOfInvestment = sumOfProportion.formatted("%.6f"), // 投资占比求和
                upperStreamId = v.upperStreamId, // 上游股东id
                level = v.level, // 层级间隔
              )
          }
          reduceLeftAndRightMap
        }
      )

      /*
       * STEP6 leftJoin，将新老信息合并
       *
       */


      val newVertexWithMulLevelInvestInfo: VertexRDD[simpleProperties] = OldGraph.vertices.leftZipJoin(nStepOfShareHolding)(
        (vid: VertexId, vd: simpleProperties, nvd: Option[Map[VertexId, simpleInvestmentInfo]]) => {

          val oldGraphInvInfo: Map[VertexId, simpleInvestmentInfo] = vd.oneStepInvInfo
          val newGraphInvInfo: Map[VertexId, simpleInvestmentInfo] = nvd.getOrElse(defaultInvestmentInfo)

          // 测试
          if (vid == 2L) {
            println("\n=========================")
            println("大鹏旧图： " + oldGraphInvInfo)
            println("大鹏新图： " + newGraphInvInfo)
          } else if (vid == 4L) {
            println("\n=========================")
            println("4右护法旧图： " + oldGraphInvInfo)
            println("4右护法新图： " + newGraphInvInfo)
          }

          // 这里简单同Key相加是有问题的，只有old和new同key的value不一致的时候才行
          val sumOfOldAndNewGraphInvInfo: Map[VertexId, simpleInvestmentInfo] = oldGraphInvInfo ++ newGraphInvInfo.map {
            case (k: VertexId, v: simpleInvestmentInfo) =>
              // 如果不一致新旧图的value不一致再相加
              // TODO 检查问题：如果在New环里面出现自己对自己的投资，相加会不会有问题
              if (v.proportionOfInvestment != oldGraphInvInfo.getOrElse(k, simpleInvestmentInfo()).proportionOfInvestment) {
                val sumOfOldAndNewGraphProportion: BigDecimal =
                  BigDecimal(v.proportionOfInvestment) +
                    BigDecimal(oldGraphInvInfo.getOrElse(k, simpleInvestmentInfo()).proportionOfInvestment)
                k -> simpleInvestmentInfo(
                  investedComName = v.investedComName // 被投资企业名称
                  , proportionOfInvestment = sumOfOldAndNewGraphProportion.formatted("%.6f") // 投资占比求和
                  , upperStreamId = v.upperStreamId // 上游股东id
                  , level = v.level // 层级间隔
                )
              } else {
                // 否则（如果一致）则直接采用旧Graph的值
                k -> simpleInvestmentInfo(
                  investedComName = v.investedComName // 被投资企业名称
                  , proportionOfInvestment = BigDecimal(v.proportionOfInvestment).formatted("%.6f") // 投资占比求和
                  , upperStreamId = v.upperStreamId // 上游股东id
                  , level = v.level // 层级间隔
                )
              }
          }
          simpleProperties(
            vd.name, // 姓名
            sumOfOldAndNewGraphInvInfo // 持股信息 新老合并
          )
        }
      )

      // 新建一张图 thirdNewGraph
      val nStepNewGraph: Graph[simpleProperties, Double] = Graph(newVertexWithMulLevelInvestInfo, graph.edges, defaultVertex)
      nStepNewGraph
    }

    /*
     * STEP5 在这里启用多层调用
     *
     * 尾递归实现
     */
    def tailFact(n: Int): Graph[simpleProperties, Double] = {
      /**
       *
       * @param n       递归次数
       * @param currRes 当前结果
       * @return 递归n次后的的Graph
       */
      @tailrec
      def loop(n: Int, currRes: Graph[simpleProperties, Double]): Graph[simpleProperties, Double] = {
        if (n == 0) return currRes
        loop(n - 1, nStepShareHoldingCalculate(currRes))
      }

      loop(n, oneStepInvInfoGraph) // loop(递归次数, 初始值)
    }

    println("\n================ 打印最终持股计算新生成的顶点 ===================\n")
    val ShareHoldingGraph: Graph[simpleProperties, Double] = tailFact(8) // 理论上递归次数增加不影响结果才是对的
    val endTime: Long = System.currentTimeMillis()
    println("\nG13运行时间： " + (endTime - startTime))
    ShareHoldingGraph.vertices.collect.foreach(println)
  }
}
