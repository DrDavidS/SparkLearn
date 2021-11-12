import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec

/**
 * Graph Demo 17 优化版本
 *
 * 本章节代码的目标是，做一次发送前后对比，如果本次和上次的Map完全一致，则不发送消息。
 *
 */

object G01_calculateStockRatio_MsgSendOnce {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkLocalConf().sc

    // 默认投资信息
    val defaultVertex = Map(99999L -> 0.00)

    /* 创建边关系，边的属性就是投资金额
    * 边的格式： Edge(srcId, dstId, attr)
    */
    val shareEdgeSeq = Seq(
      Edge(1L, 3L, 0.5), // 青毛狮子怪 -> 左护法
      Edge(2L, 4L, 0.5), // 大鹏金翅雕 -> 左护法
      Edge(3L, 4L, 0.5),
      Edge(4L, 3L, 0.5)
    )
    val shareEdgeRDD: RDD[Edge[Double]] = sc.parallelize(shareEdgeSeq)
    // 构建初始图
    val rawGraph: Graph[Map[VertexId, Double], Double] = Graph.fromEdges(shareEdgeRDD, defaultVertex)

    // 发送并且聚合消息
    val initVertex: VertexRDD[Map[VertexId, Double]] = rawGraph.aggregateMessages[Map[VertexId, Double]](
      triplet => {
        val ratio: Double = triplet.attr // 持股比例
        val dstId: VertexId = triplet.dstId // 目标ID
        val vData = Map(dstId -> ratio)

        triplet.sendToSrc(vData)
      },
      // Merge Message
      _ ++ _
    )

    // join 初始化图
    val initGraph: Graph[Map[VertexId, Double], Double] = rawGraph.outerJoinVertices(initVertex)(
      (vid: VertexId,
       vdata: Map[VertexId, Double],
       nvdata: Option[Map[VertexId, Double]]) => {
        nvdata.getOrElse(Map.empty)
      })

    def tailFact(n: Int): Graph[Map[VertexId, Double], Double] = {
      /**
       *
       * @param n       递归次数
       * @param currRes 当前结果
       * @return 递归n次后的的Graph
       */
      @tailrec
      def loop(n: Int,
               currRes: Graph[Map[VertexId, Double], Double]): Graph[Map[VertexId, Double], Double] = {
        if (n == 0) return currRes
        loop(n - 1, graphCalculate(currRes, initGraph))
      }

      loop(n, initGraph) // loop(递归次数, 初始值)
    }

    //initGraph.vertices.collect.foreach(println)
    println("进入多层计算 -> loop 20")
    val ShareHoldingGraph: Graph[Map[VertexId, Double], Double] = tailFact(20) // 理论上递归次数增加不影响结果才是对的
    println("=======  ShareHoldingGraph  ==========")
    ShareHoldingGraph.vertices.collect.foreach(println)
  }

  /**
   *
   * @param stepGraph 每次迭代输入的图
   * @param initGraph 初始图，用于Join
   * @return
   */
  def graphCalculate(stepGraph: Graph[Map[VertexId, Double], Double],
                     initGraph: Graph[Map[VertexId, Double], Double]
                    ): Graph[Map[VertexId, Double], Double] = {
    // aggregateMessages PART
    // 首先拆解图，用 aggregateMessages 计算多层级持股关系
    val msgVertexRDD: VertexRDD[Map[VertexId, Double]] = stepGraph.aggregateMessages[Map[VertexId, Double]](
      triplet => {
        val ratio: Double = triplet.attr // 持股比例
        val dstAttr: Map[VertexId, Double] = triplet.dstAttr // 目标顶点属性
        val srcAttr: Map[VertexId, Double] = triplet.srcAttr // 源顶点属性

        // 注意，这里是已经合并完毕的 dstAttr
        val vertexShareHoldingMap: Map[VertexId, Double] = dstAttr.map((kv: (VertexId, Double)) => (kv._1, kv._2 * ratio))

        /* 这里对 VertexShareHoldingMap 做一次对比
         * 从 srcAttr 中去搜寻
         * 如果不一样的才发送，否则不发送
         */
        // TODO 有问题
        vertexShareHoldingMap.map((kv: (VertexId, Double)) => {
          if (srcAttr.contains(kv._1)) {
            if (srcAttr(kv._1) == kv._2) triplet.sendToSrc(vertexShareHoldingMap)
          }
        })
      },

      // merge message 的时候会遇到同key的情况
      // 这时候不能简单合并，需要同key相加
      // https://stackoverflow.com/questions/7076128/best-way-to-merge-two-maps-and-sum-the-values-of-same-key
      (leftMap: Map[VertexId, Double], rightMap: Map[VertexId, Double]) => {
        val reduceLeftAndRightMap: Map[VertexId, Double] = leftMap ++ rightMap.map {
          case (k: VertexId, v: Double) =>
            // 左右投资比例，同key相加
            val sumRatio: Double = {
              v + leftMap.getOrElse(k, 0.00)
            }
            k -> sumRatio
        }
        reduceLeftAndRightMap
      })

    // JOIN PART
    // 将上面计算的结果Join到图里面
    val loopGraph: Graph[Map[VertexId, Double], Double] = initGraph.outerJoinVertices(msgVertexRDD)(
      (_: VertexId,
       initVerticesData: Map[VertexId, Double], // init图中顶点原有数据
       newVerticesdata: Option[Map[VertexId, Double]] // 要join的数据
      ) => {

        val nRatio: Map[VertexId, Double] = newVerticesdata.getOrElse(Map.empty) // 要join的数据【去除空值】
        val vRatio: Map[VertexId, Double] = initVerticesData
        /*
          思路：
          1. 在nRatio中做一个过滤，筛选出新增的持股信息【不在vdata里面的】
          2. 在vRatio中做一个过滤，筛选出存量的持股信息【已经存在于ndata里面的】
          3. 存量的（差异的保留） 在vRatio中做一个过滤，筛选出存量的持股信息【不存在于ndata里面的】
          4. 合并 1 2 3
         */
        val newKeyMaps: Map[VertexId, Double] = nRatio.filter((r: (VertexId, Double)) => {
          !vRatio.contains(r._1)
        })

        val reserveMaps: Map[VertexId, Double] = vRatio.filter((r: (VertexId, Double)) => {
          nRatio.contains(r._1)
        }).map((row: (VertexId, Double)) => (row._1, row._2 + nRatio(row._1)))

        val diffNN: Map[VertexId, Double] = vRatio.filter(r => {
          !nRatio.contains(r._1)
        })
        val unionMap: Map[VertexId, Double] = diffNN ++ reserveMaps ++ newKeyMaps
        unionMap
      }
    )
    loopGraph
  }
}
