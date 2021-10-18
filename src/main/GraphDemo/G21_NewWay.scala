import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}


class SparkTest {

  private def calV2(initGraph: Graph[Attr, Double]) = {
    val tinitGraph = initGraph.mapVertices((vid: VertexId, vdata: Attr) => {
      vdata.values.map(row => {
        (row._1, Map(row))
      }).toMap
    })
    println("=================")
    tinitGraph.vertices.foreach(println)
    var flagGraph = tinitGraph
    for (i <- 0 to 10) {
      val nGraph: VertexRDD[Map[VertexId, Map[VertexId, Double]]] = flagGraph.aggregateMessages[Map[VertexId, Map[VertexId, Double]]](triplet => {
        val ratio = triplet.attr // 投资比例
        val dst: Map[VertexId, Map[VertexId, Double]] = triplet.dstAttr // 目标
        val dstId = triplet.dstId // 目标id
        val src = triplet.srcAttr // 源属性
        //将dst上的信息先合并一下，理论输出节点上的持股关系时，也要合并一下
        val dd = dst.values.flatMap((_: Map[VertexId, Double]).toSeq).groupBy((_: (VertexId, Double))._1).mapValues((rr: Iterable[(VertexId, Double)]) => {
          rr.map((r1: (VertexId, Double)) => {
            r1._2 * ratio
          }).sum
        }).map((row: (VertexId, Double)) => {
          (row._1, row._2)
        })
        triplet.sendToSrc(Map(dstId -> dd))
      }, (m1, m2) => {
        m1 ++ m2
      })

      val baseGraph = tinitGraph.outerJoinVertices(nGraph)((vid, vdata, nvdata) => {
        val ndata = nvdata.getOrElse(Map.empty)
        val d = vdata.map(row => {
          var m = row._2
          if (ndata.contains(row._1)) {
            val origin = row._2
            val nn = ndata(row._1)
            //增量的持股信息
            val nKeyMaps = nn.filter(r => {
              !origin.contains(r._1)
            })
            //存量的(相同)
            val reserve = origin.filter(r => {
              nn.contains(r._1)
            }).map((row: (VertexId, Double)) => {
              //如果差异很小了，就不考虑了
              if (Math.abs(row._2 - nn(row._1)) < 0.0001) {
                row
              }
              else {
                (row._1, row._2 + nn(row._1))
              }
            })
            //存量的（差异的保留）
            val diffNN = origin.filter(r => {
              !nn.contains(r._1)
            })
            m = diffNN ++ reserve ++ nKeyMaps
          }
          (row._1, m)
        })
        d
      })
      baseGraph.vertices.foreach(row => {
        println(row)
      })
      flagGraph = baseGraph
    }
  }
}
