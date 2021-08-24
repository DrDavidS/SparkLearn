import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

/**
 * RDD 有 map, filter, reduceByKey 等操作，图也有。
 *
 * 具体的可以直接参阅源代码
 */

object G01_GraphOps {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("GraphX Ops")
    val sc = new SparkContext(sparkConf)

    // 创建顶点的RDD
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Seq((3L, ("Rxin", "student")), (7L, ("Jgonzal", "postdoc")),
        (5L, ("Franklin", "prof")), (2L, ("Istoica", "prof"))))

    // 创建边的RDD
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Seq(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

    // 定义一个默认用户，以防止与未知用户出现关系
    val defaultUser: (String, String) = ("John Doe", "Missing")
    // 初始化图
    val graph: Graph[(String, String), String] = Graph(users, relationships, defaultUser)

    // 计算入度
    val inDegrees: VertexRDD[Int] = graph.inDegrees
    inDegrees.collect().foreach(println)

    // 使用自定义的 mapUdf 处理节点
    // val newGraph = graph.mapVertices((id, attr) => mapUdf(id, attr))


    // 初始化为 PageRank
    // 给出一个图，其中顶点属性是出度
    val inputGraph: Graph[Int, String] =
      graph.outerJoinVertices(graph.outDegrees)((vid, _, degOpt) => degOpt.getOrElse(0))
    // 构建一个图，其中每个边都包含了权重，且每个顶点就是PageRank初始值
    val outputGraph: Graph[Double, Double] =
    inputGraph.mapTriplets(triplet => 1.0 / triplet.srcAttr).mapVertices((id, _) => 1.0)
  }
}
