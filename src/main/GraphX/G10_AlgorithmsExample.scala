import org.apache.spark.graphx.{EdgeTriplet, Graph, GraphLoader, PartitionStrategy, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 实战
 *
 * 假设我想从一些文本文件构建一个图，将图限制在重要关系以及重要用户，
 * 在子图上运行page-rank，然后最后返回与顶级用户关联的属性。
 * 我们可以在 GraphX 中用寥寥几行代码完成这些事情：
 *
 */

object G10_AlgorithmsExample {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("GraphX Ops")
    val sc = new SparkContext(sparkConf)

    // 读取用户数据，存到元组中。由用户id和属性列表构成
    val users: RDD[(Long, Array[String])] = sc.textFile("data/graphx/users.txt") // 读取文件
      .map((line: String) => line.split(",")) // 将用户名和数据用逗号拆分
      .map((parts: Array[String]) => (parts.head.toLong, parts.tail)) // 将头部转为long类型

    // Parse the edge data which is already in userId -> userId format
    val followerGraph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, "data/graphx/followers.txt")

    // Attach the user attributes
    val graph: Graph[Array[String], Int] = followerGraph.outerJoinVertices(users) {
      case (uid, deg, Some(attrList)) => attrList // some 获取 attrList 的值
      // 有些用户可能没有 attributes 所以把他们设置为空
      case (uid, deg, None) => Array.empty[String]
    }

    // Restrict the graph to users with usernames and names
    // 造一个子图，条件是 attr.size == 2
    val subgraph: Graph[Array[String], Int] = graph.subgraph(vpred = (vid, attr) => attr.size == 2)

    // Compute the PageRank
    val pagerankGraph: Graph[Double, Double] = subgraph.pageRank(0.001)

    // Get the attributes of the top pagerank users
    // 获取顶级 pagerank 用户的属性
    val userInfoWithPageRank: Graph[(Double, List[String]), Int] = subgraph.outerJoinVertices(pagerankGraph.vertices) {
      case (uid, attrList, Some(pr)) => (pr, attrList.toList) // 如果有pagerank那就写入列表
      case (uid, attrList, None) => (0.0, attrList.toList) // 如果没有pagerank那就记做0.0
    }
    println("================ 打印 PageRank ===================")
    println(userInfoWithPageRank.vertices.top(5)(Ordering.by(_._2._1)).mkString("\n"))
  }
}
