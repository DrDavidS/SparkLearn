import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec

/**
 * Graph Demo 15 为了语雀记录而使用
 *
 * 究极简化版，基于沐页的代码改写。
 *
 * 改进：
 * 1. 去掉了一切var变量，统统使用val
 * 2. 添加了很多注释、改写变量名，便于理解
 * 3. 采用了尾递归调用，比For更节约资源
 *
 */

object G015_yuqueData_youhua {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkLocalConf().sc

    // 默认投资信息
    val defaultVertex = Map(99999L -> Map(88888L -> 0.00))

    /* 创建边关系，边的属性就是投资金额
    * 边的格式： Edge(srcId, dstId, attr)
    */
    val shareEdgeSeq = Seq(
      Edge(1L, 6L, 1.0), // 青毛狮子怪 -> 狮驼岭集团
      Edge(6L, 4L, 1.0), // 狮驼岭集团 -> 右护法
      Edge(6L, 3L, 1.0), // 狮驼岭集团 -> 左护法
      Edge(4L, 5L, 0.675325), // 右护法 -> 小钻风
      Edge(3L, 5L, 0.324675) // 左护法 -> 小钻风
    )
    val shareEdgeRDD: RDD[Edge[Double]] = sc.parallelize(shareEdgeSeq)
    // 构建初始图
    val rawGraph: Graph[Map[VertexId, Map[VertexId, Double]], Double] = Graph.fromEdges(shareEdgeRDD, defaultVertex)

    // 发送并且聚合消息
    val initVertex = rawGraph.aggregateMessages[Map[VertexId, Map[VertexId, Double]]](
      triplet => {
        val ratio: Double = triplet.attr // 持股比例
        val dstId: VertexId = triplet.dstId // 目标ID
        val vData = Map(dstId -> Map(dstId -> ratio))

        triplet.sendToSrc(vData)
      },
      // Merge Message
      (left: Map[VertexId, Map[VertexId, Double]], right: Map[VertexId, Map[VertexId, Double]]) => {
        left ++ right
      }
    )

    // join 初始化图
    val initGraph: Graph[Map[VertexId, Map[VertexId, Double]], Double] = rawGraph.outerJoinVertices(initVertex)(
      (vid: VertexId,
       vdata: Map[VertexId, Map[VertexId, Double]],
       nvdata: Option[Map[VertexId, Map[VertexId, Double]]]) => {
        nvdata.getOrElse(Map.empty)
      })

    println("=======  initGraph  ==========")
    initGraph.vertices.foreach(println)


    def tailFact(n: Int): Graph[Map[VertexId, Map[VertexId, Double]], Double] = {
      /**
       *
       * @param n       递归次数
       * @param currRes 当前结果
       * @return 递归n次后的的Graph
       */
      @tailrec
      def loop(n: Int,
               currRes: Graph[Map[VertexId, Map[VertexId, Double]], Double]): Graph[Map[VertexId, Map[VertexId, Double]], Double] = {
        if (n == 0) return currRes
        loop(n - 1, graphCalculate(currRes, initGraph))
      }

      loop(n, initGraph) // loop(递归次数, 初始值)
    }

    //initGraph.vertices.collect.foreach(println)
    println("进入多层计算")
    val ShareHoldingGraph: Graph[Map[VertexId, Map[VertexId, Double]], Double] = tailFact(20) // 理论上递归次数增加不影响结果才是对的
    println("=======  ShareHoldingGraph  ==========")
    ShareHoldingGraph.vertices.collect.foreach(println)
  }

  /**
   * 这个函数是给尾递归调用的
   *
   * @param stepGraph 每次迭代输入的图
   * @param initGraph 初始图，用于Join
   * @return
   */
  def graphCalculate(stepGraph: Graph[Map[VertexId, Map[VertexId, Double]], Double],
                     initGraph: Graph[Map[VertexId, Map[VertexId, Double]], Double]
                    ): Graph[Map[VertexId, Map[VertexId, Double]], Double] = {
    // 首先拆解图，用 aggregateMessages 计算多层级持股关系
    val msgVertexRDD: VertexRDD[Map[VertexId, Map[VertexId, Double]]] = stepGraph.aggregateMessages[Map[VertexId, Map[VertexId, Double]]](
      triplet => {
        val ratio: Double = triplet.attr // 持股比例
        val dstAttr: Map[VertexId, Map[VertexId, Double]] = triplet.dstAttr // 目标顶点属性
        val dstId: VertexId = triplet.dstId // 目标顶点id

        // 将dst上的信息先合并一下，理论输出节点上的持股关系时，也要合并一下
        // 将各个下游邻居点发过来的，对子公司的持股比例，乘以当前边ratio后相加、合并
        // 1. 每个顶点Map的 Values，构成一个List
        val vectorValues: Iterable[Map[VertexId, Double]] = dstAttr.values
        // 2. 转换为(K, V)元组，K 为src持股子公司ID，V 为比例
        val tuples: Iterable[(VertexId, Double)] = vectorValues.flatMap(_.toSeq)
        // 3. 根据 K 分组
        val idToTuples: Map[VertexId, Iterable[(VertexId, Double)]] = tuples.groupBy(_._1)
        // 4. 对每个分组的 tuple 列表处理，首先分别计算所有发送到src顶点对子公司k的持股比例，然后计算src顶点对他们的和
        val idToDouble: Map[VertexId, Double] = idToTuples.mapValues(
          (ListOfTuples: Iterable[(VertexId, Double)]) => {
            ListOfTuples.map((tuples: (VertexId, Double)) => tuples._2 * ratio).sum
          })
        // 5. 转换回tuple，这步的意义就是序列化
        val idToDoubleSerialized: Map[VertexId, Double] = idToDouble.map(
          (row: (VertexId, Double)) => (row._1, row._2))

        // TODO 本次发送的消息和上次发送的消息差异过小，可以不发送
        triplet.sendToSrc(Map(dstId -> idToDoubleSerialized))
      },
      // merge message
      _ ++ _
  }

  )
  // nGraph.collect.foreach(println)

  // 将上面计算的结果Join到图里面
  val loopGraph: Graph[Map[VertexId, Map[VertexId, Double]], Double] = initGraph.outerJoinVertices(msgVertexRDD)(
    (vid: VertexId,
     vdata: Map[VertexId, Map[VertexId, Double]], // 图中顶点原有数据
     nvdata: Option[Map[VertexId, Map[VertexId, Double]]] // 要join的数据
    ) => {
      val ndata: Map[VertexId, Map[VertexId, Double]] = nvdata.getOrElse(Map.empty) // 要join的数据【去除空值】
      val unionData: Map[VertexId, Map[VertexId, Double]] = vdata.map((row: (VertexId, Map[VertexId, Double])) => {
        // 针对原来的图的顶点数据，做map操作

        val statrUnionMap: Map[VertexId, Double] = row._2
        val newValue: Map[VertexId, Double] = if (ndata.contains(row._1)) // 如果当前顶点ID在被join的ndata中（说明发送的顶点相同）
        {
          val vRatio: Map[VertexId, Double] = row._2 // vdata中的持股比例
          val nRatio: Map[VertexId, Double] = ndata(row._1) // 同时在ndata中找到新计算的持股比例

          // 增量的持股信息
          // 1. 在nRatio中做一个过滤，筛选出新增的持股信息【不在vdata里面的】
          // TODO 这里是不是能优化一下
          val newKeyMaps: Map[VertexId, Double] = nRatio.filter((r: (VertexId, Double)) => {
            !vRatio.contains(r._1)
          })

          // 2. 在vRatio中做一个过滤，筛选出存量的持股信息【已经存在于ndata里面的】
          val reserveMaps: Map[VertexId, Double] = vRatio.filter((r: (VertexId, Double)) => {
            nRatio.contains(r._1)
          }).map((row: (VertexId, Double)) => (row._1, row._2 + nRatio(row._1)))

          // 3. 存量的（差异的保留） 在vRatio中做一个过滤，筛选出存量的持股信息【不存在于ndata里面的】
          val diffNN: Map[VertexId, Double] = vRatio.filter(r => {
            !nRatio.contains(r._1)
          })
          val unionMap: Map[VertexId, Double] = diffNN ++ reserveMaps ++ newKeyMaps
          unionMap
        } else statrUnionMap // 节点对子公司的持股比例【原始】
        (row._1, newValue)
      })
      unionData
    })
  // loopGraph.vertices.collect.foreach(println)
  loopGraph // 返回的Graph
}
}
