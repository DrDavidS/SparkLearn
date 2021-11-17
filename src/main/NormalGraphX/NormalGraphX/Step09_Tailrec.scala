package NormalGraphX

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec

/**
 * Step09 递归调用
 *
 * 对一个函数重复调用，且下次的输入就是上次的输出，不是恰好可以写成递归的形式吗？
 *
 * 我们选择尾递归的方式来写。
 *
 * 尾递归和普通递归有什么区别？有什么优势？参考：
 * https://www.zhihu.com/question/20761771
 *
 * 在本端代码中，我们计算持股是不需要回溯的，
 * 换句话说，下一次的计算仅仅依赖上次的结果，因此选择尾递归可以节约内存空间。
 *
 * 题外话：为什么用尾递归，不用for循环。
 * 首先尾递归和线性递归的区别，之前说得很清楚了。
 * 而for循环的问题在于，编写for循环的时候，很容易引入 var 变量。
 * 为了保证整个代码的变量都是 val 变量，我们这里没有使用 for 循环。
 *
 * */

object Step09_Tailrec {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkLocalConf().sc

    val vertexSeq = Seq(
      (1L, ("青毛狮子怪", Map(6L -> 1.0))),
      (3L, ("左护法", Map(5L -> 0.324675))),
      (4L, ("右护法", Map(5L -> 0.675325))),
      (5L, ("小钻风", Map(999999999L -> 0.0))),
      (6L, ("狮驼岭集团", Map(3L -> 1.0, 4L -> 1.0)))
    )
    val vertexSeqRDD: RDD[(VertexId, (String, Map[VertexId, Double]))] = sc.parallelize(vertexSeq)

    val shareEdgeSeq = Seq(
      Edge(1L, 6L, 1.00), // 青毛狮子怪 -> 狮驼岭集团
      Edge(6L, 4L, 1.00), // 狮驼岭集团 -> 右护法
      Edge(6L, 3L, 1.00), // 狮驼岭集团 -> 左护法
      Edge(4L, 5L, 0.675325), // 右护法 -> 小钻风
      Edge(3L, 5L, 0.324675) // 左护法 -> 小钻风
    )
    val shareEdgeRDD: RDD[Edge[Double]] = sc.parallelize(shareEdgeSeq)

    // 构建初始图
    val rawGraph: Graph[(String, Map[VertexId, Double]), Double] = Graph(vertexSeqRDD, shareEdgeRDD)

    /**
     * n阶计算函数，用于递归调用。
     *
     * @param inGraph 输入的图，节点上是当前持股信息。
     * @return 输出的图。包含了向下一层计算的持股信息。
     */
    def nStepCalculate(inGraph: Graph[(String, Map[VertexId, Double]), Double],
                       initGraph: Graph[(String, Map[VertexId, Double]), Double]
                      ): Graph[(String, Map[VertexId, Double]), Double] = {
      val capitalOfVertex: VertexRDD[Map[VertexId, Double]] = inGraph.aggregateMessages[Map[VertexId, Double]](
        (triplet: EdgeContext[(String, Map[VertexId, Double]), Double, Map[VertexId, Double]]) => {
          val dstShareHoldingMap: Map[VertexId, Double] = triplet.dstAttr._2 // 目标点当前的持股Map
          val ratio: Double = triplet.attr // 源点对目标点的持股比例

          val vertexShareHoldingMap: Map[VertexId, Double] = dstShareHoldingMap.map(
            (kv: (VertexId, Double)) => {
              (kv._1, kv._2 * ratio)
            })

          triplet.sendToSrc(vertexShareHoldingMap)
        },
        // mergeMsg
        // 这里用 _ ++ _ 会出问题
        // 转而采用 同key 相加的方法
        (leftMap, rightMap) => {
          val reduceLeftAndRightMap = leftMap ++ rightMap.map {
            case (k: VertexId, v: Double) =>
              // 左右投资比例，同key相加
              val sumRatio: Double = {
                v + leftMap.getOrElse(k, 0.00)
              }
              k -> sumRatio
          }
          reduceLeftAndRightMap
        }
      )

      val outGraph: Graph[(String, Map[VertexId, Double]), Double] = initGraph.outerJoinVertices(capitalOfVertex)(
        (_: VertexId, vdata: (String, Map[VertexId, Double]), nvdata: Option[Map[VertexId, Double]]) => {
          val newVertexDataMap: Map[VertexId, Double] = nvdata.getOrElse(Map(999999999L -> 0.00))
          (vdata._1, vdata._2 ++ newVertexDataMap) // (名字，Map 合并）
        }
      )
      outGraph
    }

    def TailFact(n: Int,
                 initGraph: Graph[(String, Map[VertexId, Double]), Double]
                ): Graph[(String, Map[VertexId, Double]), Double] = {
      /**
       * 递归
       *
       * @param n       递归次数
       * @param currRes 当前结果
       * @return 递归n次后的的Graph
       */
      @tailrec
      def loop(n: Int,
               currRes: Graph[(String, Map[VertexId, Double]), Double]
              ): Graph[(String, Map[VertexId, Double]), Double] = {
        if (n == 0) return currRes
        loop(n - 1, nStepCalculate(currRes, initGraph))
      }

      loop(n, initGraph) // loop(递归次数, 初始值)
    }

    val resGraph = TailFact(5, rawGraph)
    println("============== 5层 ====================")
    resGraph.vertices.collect.foreach(println)
  }
}
