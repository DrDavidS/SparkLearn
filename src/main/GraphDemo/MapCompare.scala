object MapCompare {
  def main(args: Array[String]): Unit = {
    val dt: String = "20211101"
    val naturalPerson2Corp: String =
      s"""SELECT srcid,dstid,stock_ratio
         |FROM ant_zmc.adm_zmep_graph_human2com_edge_df
         |WHERE dt=$dt
         |""".stripMargin
    println(naturalPerson2Corp)
  }
}
