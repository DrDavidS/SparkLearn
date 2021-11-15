import org.apache.spark.graphx.{EdgeContext, Graph, VertexId, VertexRDD}

object GraphCalculate {
  /**
   * 这个函数是给尾递归调用的，用于旧版的复杂数据结构计算
   *
   * @param stepGraph 每次迭代输入的图
   * @param initGraph 初始图，用于Join
   * @return loopGraph
   */
  def complexGraphCalculate(stepGraph: Graph[Map[VertexId, Map[VertexId, Double]], Double],
                            initGraph: Graph[Map[VertexId, Map[VertexId, Double]], Double]
                           ): Graph[Map[VertexId, Map[VertexId, Double]], Double] = {
    // 首先拆解图，用 aggregateMessages 计算多层级持股关系
    val msgVertexRDD: VertexRDD[Map[VertexId, Map[VertexId, Double]]] = stepGraph.aggregateMessages[Map[VertexId, Map[VertexId, Double]]](
      (triplet: EdgeContext[Map[VertexId, Map[VertexId, Double]], Double, Map[VertexId, Map[VertexId, Double]]]) => {
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
    )

    // 将上面计算的结果Join到图里面
    val loopGraph: Graph[Map[VertexId, Map[VertexId, Double]], Double] = initGraph.outerJoinVertices(msgVertexRDD)(
      (_: VertexId,
       vdata: Map[VertexId, Map[VertexId, Double]], // 图中顶点原有数据
       nvdata: Option[Map[VertexId, Map[VertexId, Double]]] // 要join的数据
      ) => {
        val ndata: Map[VertexId, Map[VertexId, Double]] = nvdata.getOrElse(Map.empty) // 要join的数据【去除空值】
        val unionData: Map[VertexId, Map[VertexId, Double]] = vdata.map((row: (VertexId, Map[VertexId, Double])) => {
          // 针对原来的图的顶点数据，做map操作

          val startUnionMap: Map[VertexId, Double] = row._2
          val newValue: Map[VertexId, Double] = if (ndata.contains(row._1)) // 如果当前顶点ID在被join的ndata中（说明发送的顶点相同）
          {
            val vRatio: Map[VertexId, Double] = row._2 // vdata中的持股比例
            val nRatio: Map[VertexId, Double] = ndata(row._1) // 同时在ndata中找到新计算的持股比例

            // 增量的持股信息
            // 1. 在nRatio中做一个过滤，筛选出新增的持股信息【不在vdata里面的】
            val newKeyMaps: Map[VertexId, Double] = nRatio.filter((r: (VertexId, Double)) => {
              !vRatio.contains(r._1)
            })

            // 2. 在vRatio中做一个过滤，筛选出存量的持股信息【已经存在于ndata里面的】
            val reserveMaps: Map[VertexId, Double] = vRatio.filter((r: (VertexId, Double)) => {
              nRatio.contains(r._1)
            }).map((row: (VertexId, Double)) => (row._1, row._2 + nRatio(row._1)))

            // 3. 存量的（差异的保留） 在vRatio中做一个过滤，筛选出存量的持股信息【不存在于ndata里面的】
            val diffNN: Map[VertexId, Double] = vRatio.filter(
              (r: (VertexId, Double)) => {
                !nRatio.contains(r._1)
              })
            val unionMap: Map[VertexId, Double] = diffNN ++ reserveMaps ++ newKeyMaps
            unionMap
          } else startUnionMap // 节点对子公司的持股比例【原始】
          (row._1, newValue)
        })
        unionData
      })
    loopGraph // 返回的Graph
  }

  /**
   * 这个函数是给尾递归调用的，用于新版的简单数据结构计算
   *
   * @param stepGraph 每次迭代输入的图
   * @param initGraph 初始图，用于Join
   * @return loopGraph
   */
  def simpleGraphCalculate(stepGraph: Graph[Map[VertexId, Double], Double],
                           initGraph: Graph[Map[VertexId, Double], Double]
                          ): Graph[Map[VertexId, Double], Double] = {
    // aggregateMessages PART
    // 首先拆解图，用 aggregateMessages 计算多层级持股关系
    val msgVertexRDD: VertexRDD[Map[VertexId, Double]] = stepGraph.aggregateMessages[Map[VertexId, Double]](
      (triplet: EdgeContext[Map[VertexId, Double], Double, Map[VertexId, Double]]) => {
        val ratio: Double = triplet.attr // 持股比例
        val dstAttr: Map[VertexId, Double] = triplet.dstAttr // 目标顶点属性
        val srcAttr: Map[VertexId, Double] = triplet.srcAttr // 源顶点属性

        // 注意，这里是已经合并完毕的 dstAttr
        val vertexShareHoldingMap: Map[VertexId, Double] = dstAttr.map(
          (kv: (VertexId, Double)) => {
            (kv._1, kv._2 * ratio)
          })

        /* 这里对 VertexShareHoldingMap 做一次对比
         * 从 srcAttr 中去搜寻
         * if: srcAttr 中没有 --> 发送
         * if: srcAttr 中有
         *      if srcAttr 中不一致 --> 发送
         *      if srcAttr 中一致 --> 不发送
         *
         * 虽然直接 triplet.sendToSrc(vertexShareHoldingMap) 也是可以的，但是这样能节约一点点消息发送的资源
         *
         */

        vertexShareHoldingMap.map((kv: (VertexId, Double)) => {
          if (srcAttr.contains(kv._1)) {
            if (srcAttr(kv._1) != kv._2) {
              triplet.sendToSrc(Map(kv._1 -> kv._2))
            }
          } else {
            triplet.sendToSrc(Map(kv._1 -> kv._2))
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
       newVerticesData: Option[Map[VertexId, Double]] // 要join的数据
      ) => {

        val nRatio: Map[VertexId, Double] = newVerticesData.getOrElse(Map.empty) // 要join的数据【去除空值】
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

        val diffNN: Map[VertexId, Double] = vRatio.filter(
          (r: (VertexId, Double)) => {
            !nRatio.contains(r._1)
          })
        val unionMap: Map[VertexId, Double] = diffNN ++ reserveMaps ++ newKeyMaps
        unionMap
      }
    )
    loopGraph
  }

}
