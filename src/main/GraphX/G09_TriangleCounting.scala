import org.apache.spark.graphx.{Graph, GraphLoader, PartitionStrategy, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 当一个顶点有两个相邻的顶点，且这两个相邻顶点之间也有一条边时，它就是三角形的一部分。
 * GraphX 在 TriangleCount object 中实现了一个三角形计数算法，该算法确定通过每个顶点的三角形数量，提供聚类的度量。
 * 我们在 PageRank section 计算了社交网络数据集的三角形计数。
 *
 * 注意，TriangleCount 要求边处于规范方向（srcId < dstId）并且使用 Graph.partitionBy 对图进行分区。
 *
 */

object G09_TriangleCounting {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("GraphX Ops")
    val sc = new SparkContext(sparkConf)

    // 按照规范顺序加载边并划分图，以便获得三角形计数
    val graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc,
      "data/graphx/followers.txt",
      canonicalOrientation = true)
      .partitionBy(PartitionStrategy.RandomVertexCut)

    // Find the triangle count for each vertex
    // 找到每个顶点的三角形计数
    val triCounts: VertexRDD[Int] = graph.triangleCount().vertices

    // Join the triangle counts with the usernames
    // 将三角形计数和用户名join起来
    // 用户名格式：顶点id, 用户名
    val users: RDD[(Long, String)] = sc.textFile("data/graphx/users.txt").map {
      line: String =>
        val fields: Array[String] = line.split(",")
        (fields(0).toLong, fields(1))
    }

    val triCountByUsername: RDD[(String, Int)] = users.join(triCounts).map {
      case (id, (username, tc)) =>
        (username, tc)
    }
    // Print the result
    println(triCountByUsername.collect().mkString("\n"))
  }
}
