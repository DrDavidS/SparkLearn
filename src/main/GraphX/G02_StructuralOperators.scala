import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 结构运算符
 *
 * 目前 GraphX 仅支持一组简单的常用结构运算符，我们希望在未来添加更多。
 *
 * 注意看源代码，构建图的时候用的是 Graph，头三个参数分别是：
 *    vertices: RDD[(VertexId, VD)], the "set" of vertices and their attributes
      edges: RDD[Edge[ED]], the collection of edges in the graph
      defaultVertexAttr: VD = null.asInstanceOf[VD], the default vertex attribute to use for vertices that are mentioned in edges but not in vertices
 *
 *
 */

object G02_StructuralOperators {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("GraphX Ops")
    val sc = new SparkContext(sparkConf)

    // 为顶点创建RDD
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Seq((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof")),
        (4L, ("peter", "student"))))

    // 为边创建RDD
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Seq(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi"),
        Edge(4L, 0L, "student"), Edge(5L, 0L, "colleague")))

    // 创建默认用户
    val defaultUser: (String, String) = ("John Doe", "Missing")

    // 构建初始图
    val graph: Graph[(String, String), String] = Graph(users, relationships, defaultUser)

    // 注意：这里有个用户0（我们没有什么信息）连接到4 (peter) 和 5 (franklin).
    println("================ 打印三元组关系 ===================")
    graph.triplets.map(
      (triplet: EdgeTriplet[(String, String), String]) =>
        triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println)

    // 移除丢失的顶点，以及指向他们的边
    // 这里采用了子图的形式，然后把参数为 "Missing" 的干掉了
    val validGraph: Graph[(String, String), String] = graph.subgraph(
      vpred = (id: VertexId, attr: (String, String)) =>
        attr._2 != "Missing")
    // 通过移除用户0， 取消了用户 4 和 5 的连接
    println("================ 打印边 ===================")
    validGraph.vertices.collect.foreach(println)
    println("================ 打印三元组关系-去掉无用信息 ===================")
    validGraph.triplets.map(
      (triplet: EdgeTriplet[(String, String), String]) =>
        triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println)
  }
}
