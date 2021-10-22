import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.sql.SparkSession
import org.scalatest.Outcome
import org.scalatest.funsuite.FixtureAnyFunSuite

case class Attr(neigh: List[Long], values: Map[Long, Double])

class SparkTest extends FixtureAnyFunSuite{

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
      Edge(1L, 6L, 1.0), // 青毛狮子怪 -> 狮驼岭集团
      Edge(6L, 4L, 1.0), // 狮驼岭集团 -> 右护法
      Edge(6L, 3L, 1.0), // 狮驼岭集团 -> 左护法
      Edge(4L, 5L, 0.675325), // 右护法 -> 小钻风
      Edge(3L, 5L, 0.324675) // 左护法 -> 小钻风
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
      if (nvdata.isEmpty)
        Attr(List.empty, Map.empty)
      else
        nvdata.get
    })
    //初始化基础图的数据
    initGraph.vertices.foreach(row => {
      println(row)
    })
    calV2(initGraph)  // 基于初始化图，开始循环计算
  }

  private def calV2(initGraph: Graph[Attr, Double]) = {
    val tinitGraph = initGraph.mapVertices((vid, vdata) => {
      vdata.values.map(row => {
        (row._1, Map(row))
      }).toMap
    })
    println("=================")
    tinitGraph.vertices.foreach(println)
    var flagGraph = tinitGraph
    for (i <- 0 to 10) {
      val nGraph = flagGraph.aggregateMessages[Map[VertexId, Map[VertexId, Double]]](triplet => {
        val ratio = triplet.attr
        val dst = triplet.dstAttr
        val dstId = triplet.dstId
        val src = triplet.srcAttr
        //将dst上的信息先合并一下，理论输出节点上的持股关系时，也要合并一下
        val dd = dst.values.flatMap(_.toSeq).groupBy(_._1).mapValues(rr => {
          rr.map(r1 => {
            r1._2 * ratio
          }).sum
        }).map(row => {
          (row._1, row._2)
        })
        //val originDst = src.get(dstId).get //原来这个目标点发过来什么消息，先拿过来作为BASE
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
            val nn = ndata.get(row._1).get
            //增量的持股信息
            val nKeyMaps = nn.filter(r => {
              !origin.contains(r._1)
            })
            //存量的(相同)
            val reserve = origin.filter(r => {
              nn.contains(r._1)
            }).map(row => {
              //如果差异很小了，就不考虑了
              if (Math.abs(row._2 - nn.get(row._1).get) < 0.0001) {
                row
              }
              else {
                (row._1, row._2 + nn.get(row._1).get)
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
      baseGraph.vertices.foreach((row: (VertexId, Map[VertexId, Map[VertexId, Double]])) => {
        println(row)
      })
      flagGraph = baseGraph
    }
  }
}
