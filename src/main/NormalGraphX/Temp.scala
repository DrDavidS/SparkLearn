package NormalGraphX

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.graphx.Edge

import com.fasterxml.jackson.module.scala.DefaultScalaModule

// 测试用例
object Temp {
  def main(args: Array[String]): Unit = {
    val TshareEdgeSeq = Seq(
      Edge(1L, 6L, 1.0), // 青毛狮子怪 -> 狮驼岭集团
      Edge(6L, 4L, 1.0), // 狮驼岭集团 -> 右护法
      Edge(6L, 3L, 1.0), // 狮驼岭集团 -> 左护法
      Edge(4L, 5L, 0.675325), // 右护法 -> 小钻风
      Edge(3L, 5L, 0.324675) // 左护法 -> 小钻风
    )

    val shareEdgeSeq = Seq(
      Edge(1L, 3L, 0.5), // 青毛狮子怪 -> 左护法
      Edge(2L, 4L, 0.5), // 大鹏金翅雕 -> 左护法
      Edge(3L, 4L, 0.5),
      Edge(4L, 3L, 0.5)
    )

    val tsetMap = Map(9999L -> 0.500)
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val res: String = mapper.writeValueAsString(tsetMap)
    println(res)


  }
}
