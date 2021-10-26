import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.sql.SparkSession
import org.scalatest.Outcome
import org.scalatest.funsuite.FixtureAnyFunSuite

case class Attr(neigh: List[Long], values: Map[Long, Double])

class SparkTest extends FixtureAnyFunSuite {

  override type FixtureParam = SparkSession

  override def withFixture(test: OneArgTest): Outcome = {
    val sparkSession = SparkSession.builder
      .appName("Test-Spark-Local")
      .master("local[2]")
      .getOrCreate()
    try {
      withFixture(test.toNoArgTest(sparkSession))
    } finally sparkSession.stop
  }

  test("stockholder") { spark =>
    val seq = Seq(
      Edge(1L, 3L, 0.5), // 青毛狮子怪 -> 左护法
      Edge(2L, 4L, 0.5), // 大鹏金翅雕 -> 左护法
      Edge(3L, 7L, 1.0),
      Edge(7L, 4L, 0.5),
      Edge(4L, 10L, 1.0),
      Edge(10L, 3L, 0.5),
    )
    val edgeRDD = spark.sparkContext.parallelize(seq)
    val graph = Graph.fromEdges(edgeRDD, Attr(List.empty, Map.empty))
    val initVertex = graph.aggregateMessages[Attr](triplet => {

      val ratio = triplet.attr
      val dstId = triplet.dstId
      val neigh = List(dstId)
      val values = Map(dstId -> ratio)
      triplet.sendToSrc(Attr(neigh, values))

    }, // merge Message
      (m1, m2) => {
        Attr(m1.neigh ++ m2.neigh, m1.values ++ m2.values)
      })

    // join 初始化图
    val initGraph = graph.outerJoinVertices(initVertex)((vid, vdata, nvdata) => {
      nvdata.getOrElse(Attr(List.empty, Map.empty))
    })
    //初始化基础图的数据
    initGraph.vertices.foreach(row => {
      // println(row)
    })
    val resGraph = calV2(initGraph) // 基于初始化图，开始循环计算
    println("=======  resGraph  ==========")
    resGraph.vertices.foreach(println)
  }

  private def calV2(initGraph: Graph[Attr, Double]): Graph[Map[VertexId, Map[VertexId, Double]], Double] = {
    val tinitGraph: Graph[Map[VertexId, Map[VertexId, Double]], Double] = initGraph.mapVertices((vid: VertexId, vdata: Attr) => {
      vdata.values.map(row => {
        (row._1, Map(row))
      })
    })
    println("=======  tinitGraph  ==========")
    tinitGraph.vertices.foreach(println)
    var flagGraph = tinitGraph
    for (i <- 0 to 20) {
      val nGraph: VertexRDD[Map[VertexId, Map[VertexId, Double]]] = flagGraph.aggregateMessages[Map[VertexId, Map[VertexId, Double]]](
        triplet => {
          val ratio: Double = triplet.attr
          val dst: Map[VertexId, Map[VertexId, Double]] = triplet.dstAttr
          val dstId: VertexId = triplet.dstId

          //将dst上的信息先合并一下，理论输出节点上的持股关系时，也要合并一下
          val dd: Map[VertexId, Double] = dst.values.flatMap(_.toSeq).groupBy(_._1).mapValues(rr => {
            rr.map(r1 => {
              r1._2 * ratio
            }).sum
          }).map(row => {
            (row._1, row._2)
          })

          triplet.sendToSrc(Map(dstId -> dd))
        }, (m1, m2) => {
          m1 ++ m2
        })

      val baseGraph = tinitGraph.outerJoinVertices(nGraph)((vid, vdata, nvdata) => {
        val ndata = nvdata.getOrElse(Map.empty)
        val unionData = vdata.map(row => {

          val unionMap = if (ndata.contains(row._1)) {
            val origin = row._2
            val nn = ndata(row._1)
            //增量的持股信息
            val nKeyMaps = nn.filter(r => {
              !origin.contains(r._1)
            })
            //存量的(相同)
            val reserve = origin.filter(r => {
              nn.contains(r._1)
            }).map(row => {
              //如果差异很小了，就不考虑了
              if (Math.abs(row._2 - nn(row._1)) < 0.01) {
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
            diffNN ++ reserve ++ nKeyMaps
          } else row._2
          (row._1, unionMap)
        })
        unionData
      })
      baseGraph.vertices.foreach((row: (VertexId, Map[VertexId, Map[VertexId, Double]])) => {
        // println(row)
      })
      flagGraph = baseGraph
    }
    flagGraph
  }
}
