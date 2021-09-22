import org.apache.spark.graphx.VertexId

case class baseProperties(
                           name: String, // 名称
                           invType: String, // 类型，比如自然人、法人、政府机关
                           age: String, // 年龄
                           registeredCapital: BigDecimal = 0.0, // 总注册资本
                           oneStepInvInfo: Map[VertexId, investmentInfo] // N级投资对象的持股信息
                         )

case class investmentInfo(
                           investedComName: String = "default_name", // 被投资企业的名称，和Key对应
                           proportionOfInvestment: String = "0.00", // 投资占比
                           registeredCapital: BigDecimal = 0, // 被投资对象的总注册资本
                           upperStreamId: VertexId = 99998L, // 此对象的上游对象（投资方）
                           level: Int = 1 // 距离当前节点的层级
                         )