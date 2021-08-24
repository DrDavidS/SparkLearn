import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
//Importing the necessary classes
import org.apache.spark.graphx._

/**
 * 创建一个最简单的图
 *  1. 创建顶点RDD ——
 *     顶点RDD的类型要求为 RDD[(VertexId, (String, String)) ] ，其中VertexId是long类型
 *
 * 2. 创建边RDD ——
 * 边RDD的类型要求为 RDD[Edge[String] ]
 *
 * 3. 创建图
 *
 * 构建边的时候使用了 Edge case 类，边具有对应于源和目标顶点标识符的 srcId 和 dstId.
 * Edge类还有一个 attr 成员，用于存储 edge 属性
 *
 * 我们可以使用 graph.vertices 和 graph.edges 成员将图解构为顶点和边视图
 * 可以用 filter 来查询相关的点和边
 * 也可以打印成员之间的三元组关系
 */

object G00_HelloGraph {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("GraphX")
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

    // 打印点和边
    println("================= 打印点和边 =================")
    graph.vertices.collect().foreach(println)
    graph.edges.collect().foreach(println)

    // 采用点和边的filter来计数或者或打印特定属性的点和边
    // println(graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count)
    // Count all the edges where src > dst
    // println(graph.edges.filter(e => e.srcId > e.dstId).count)

    // 打印三元组关系
    println("================= 打印三元组关系 =================")
    val facts: RDD[String] =
      graph.triplets.map((triplet: EdgeTriplet[(String, String), String]) =>
        triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
    facts.collect.foreach(println)
  }
}