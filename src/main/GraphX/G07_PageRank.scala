import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, GraphLoader, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * PageRank
 *
 * https://github.com/apache/spark/blob/0494dc90af48ce7da0625485a4dc6917a244d580/examples/src/main/scala/org/apache/spark/examples/graphx/PageRankExample.scala
 *
 * 如何使用PageRank
 * 这里借用源代码的example做实验。这个txt文件在：https://github.com/apache/spark/blob/master/data/graphx/followers.txt
 *
 * 需要首先学习一下PageRank的算法
 */

object G07_PageRank {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("GraphX Ops")
    val sc = new SparkContext(sparkConf)

    // 读取边信息，构建图
    val graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, "data/graphx/followers.txt")

    // 运行PageRank
    val ranks: VertexRDD[Double] = graph.pageRank(0.0001).vertices

    // 将ranks和用户名join起来
    val users: RDD[(VertexId, String)] = sc.textFile("data/graphx/users.txt").map {
      line: String =>
        val fields: Array[String] = line.split(",")
        (fields(0).toLong, fields(1))
    }

    val ranksByUsername: RDD[(String, Double)] = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }
    // 打印结果
    println(ranksByUsername.collect().mkString("\n"))
  }
}
