import org.apache.spark.graphx.{Graph, VertexId}

import scala.annotation.tailrec

/**
 * 尾递归专用
 */
object TailFact {
  def simpleTailFact(n: Int,
               initGraph: Graph[Map[VertexId, Double], Double]
              ): Graph[Map[VertexId, Double], Double] = {
    /**
     * 简单数据结构
     * @param n       递归次数
     * @param currRes 当前结果
     * @return 递归n次后的的Graph
     */
    @tailrec
    def loop(n: Int,
             currRes: Graph[Map[VertexId, Double], Double]
            ): Graph[Map[VertexId, Double], Double] = {
      if (n == 0) return currRes
      loop(n - 1, GraphCalculate.simpleGraphCalculate(currRes, initGraph))
    }
    loop(n, initGraph) // loop(递归次数, 初始值)
  }

  def complexTailFact(n: Int,
               initGraph: Graph[Map[VertexId, Map[VertexId, Double]], Double]
              ): Graph[Map[VertexId, Map[VertexId, Double]], Double] = {
    /**
     * 复杂数据结构
     *
     * @param n       递归次数
     * @param currRes 当前结果
     * @return 递归n次后的的Graph
     */
    @tailrec
    def loop(n: Int,
             currRes: Graph[Map[VertexId, Map[VertexId, Double]], Double]
            ): Graph[Map[VertexId, Map[VertexId, Double]], Double] = {
      if (n == 0) return currRes
      loop(n - 1, GraphCalculate.complexGraphCalculate(currRes, initGraph))
    }
    loop(n, initGraph) // loop(递归次数, 初始值)
  }
}
