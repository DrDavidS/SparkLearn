import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Join运算符 —— 待补充
 *
 * 很多时候，我们需要将来自外部集合 (RDD) 的数据与图做join。
 * 例如，我们可能有额外的用户属性想要与现有图合并，
 * 或者我们可能想要将顶点属性从一个图中拉到另一个图中。
 * 这些任务可以使用join运算符来完成。
 *
 */

object G03_JoinOperators {
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

  }
}
