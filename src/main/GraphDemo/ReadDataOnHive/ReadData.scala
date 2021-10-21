package ReadDataOnHive

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class ReadData(sparkSession: SparkSession, op: Commands) {
  def readHive: Graph[None.type, Double] = {
    import sparkSession.implicits._

    // from Hive
    // 传入日期dt参数
    val dt: String = op.dt

    // 自然人到公司边
    val naturalPerson2Corp: String =
      s"""
         |SELECT srcid
         |      ,dstid
         |      ,stock_ratio
         |FROM  ant_zmc.adm_zmep_graph_human2com_edge_df
         |WHERE dt=${dt}
         |""".stripMargin
    // 公司到公司边
    val corp2Corp: String =
      s"""
         |SELECT srcid
         |      ,dstid
         |      ,stock_ratio
         |FROM  ant_zmc.adm_zmep_graph_com2com_edge_df
         |WHERE dt=${dt}
         |""".stripMargin

    /**
     *
     * @param row 读取Hive表的每一行记录
     * @return
     */
    def extractRow(row: Row): Edge[Double] = {
      val srcId: VertexId = row.getAs[VertexId]("srcid")
      val dstId: VertexId = row.getAs[VertexId]("dstid")
      val r = row.getAs[String]("stock_ratio")
      var stockRatio = 0.0
      if (r != null)
        stockRatio = r.toDouble
      Edge(srcId, dstId, stockRatio)
    }

    val P2CData: DataFrame = sparkSession.sql(naturalPerson2Corp)
    P2CData.show(100) // 展示前面100条
    // 读取自然人到公司
    val edgeNaturalPerson2Corp = P2CData.map((row: Row) => {
      extractRow(row)
    })
    // 读取公司到公司边表的数据
    val edgeCorp2Corp = sparkSession.sql(corp2Corp).map((row: Row) => {
      extractRow(row)
    })
    val graphDataRDD: RDD[Edge[Double]] = edgeNaturalPerson2Corp.
      union(edgeCorp2Corp).
      repartition(1000).rdd

    //构造图
    Graph.fromEdges(graphDataRDD, None)
  }
}
