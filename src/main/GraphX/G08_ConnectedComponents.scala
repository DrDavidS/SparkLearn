import org.apache.spark.graphx.{Graph, GraphLoader, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 连通分量
 *
 * 连通分量算法用其编号最小的顶点的 ID 标记图的每个连通分量。
 * 例如，在社交网络中，连通分量可以近似于聚类。 GraphX 在 ConnectedComponents object中有算法的实现，
 * 我们从 PageRank 部分中计算示例社交网络数据集的连通分量，如下所示：
 *
 */

object G08_ConnectedComponents {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("GraphX Ops")
    val sc = new SparkContext(sparkConf)

    // 从 PageRank 例子中读取图
    val graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, "data/graphx/followers.txt")

    // 计算连通分量
    val cc: VertexRDD[VertexId] = graph.connectedComponents().vertices

    // 将ranks和用户名join起来
    val users: RDD[(VertexId, String)] = sc.textFile("data/graphx/users.txt").map {
      line: String =>
        val fields: Array[String] = line.split(",")
        (fields(0).toLong, fields(1))
    }

    val ccByUsername: RDD[(String, VertexId)] = users.join(cc).map {
      case (id, (username, cc)) => (username, cc)
    }
    // Print the result
    println(ccByUsername.collect().mkString("\n"))
  }
}
