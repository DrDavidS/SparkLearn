import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{EdgeTriplet, Graph, VertexId}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Pregel API
 *
 * 使用 Pregel 算子来表达计算，例如单源最短路径。
 */

object G06_Pregel {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("GraphX Ops")
    val sc = new SparkContext(sparkConf)

    // A graph with edge attributes containing distances
    val graph: Graph[Long, Double] =
      GraphGenerators.logNormalGraph(sc, numVertices = 100).mapEdges(e => e.attr.toDouble)

    val sourceId: VertexId = 42 // The ultimate source

    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph: Graph[Double, Double] = graph.mapVertices((id, _) =>
      if (id == sourceId) 0.0 else Double.PositiveInfinity)

    val sssp: Graph[Double, Double] = initialGraph.pregel(Double.PositiveInfinity)(
      (id: VertexId, dist: Double, newDist: Double) =>
        math.min(dist, newDist), // Vertex Program
      (triplet: EdgeTriplet[Double, Double]) => { // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message
    )
    println(sssp.vertices.collect.mkString("\n"))
  }
}
