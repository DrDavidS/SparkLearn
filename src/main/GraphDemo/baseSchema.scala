import org.apache.spark.graphx.VertexId

case class baseProperties(
                           name: String, // 名称
                           registeredCapital: BigDecimal = 0.0, // 总注册资本
                           oneStepInvInfo: Map[VertexId, investmentInfo] // N级投资对象的持股信息
                         )

case class investmentInfo(
                           investedComName: String = "default_name", // 被投资企业的名称，和Key对应
                           proportionOfInvestment: String = "0.00", // 投资占比
                           registeredCapital: BigDecimal = 0, // 被投资对象的总注册资本
                           upperStreamId: VertexId = 99998L, // 此对象的上游对象（投资方）
                           level: Int = 1, // 距离当前节点的层级
                           addSign: Boolean = false, // 属于Src节点信息，避免重复相加
                         )

case class simpleProperties(
                             name: String, // 名称
                             nStepInvInfo: Map[VertexId, simpleInvestmentInfo], // N级投资对象的持股信息
                           )

case class simpleInvestmentInfo(
                                 investedComName: String = "default_name", // 被投资企业的名称，和Key对应
                                 investmentMap: Map[VertexId, BigDecimal] = Map(99999L -> BigDecimal(0.00000)), // 投资占比
                               )

//case class Attr(
//                 neigh: List[VertexId],
//                 values: Map[VertexId, Double]
//               )