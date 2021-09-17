import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Graph Demo第四步：顶点消息的聚合
 *
 * 首先，本节我们给顶点加一个属性，叫做年龄。
 *
 * 本次实验的目的是计算投资人的平均年龄，依旧采用 aggregateMessages 函数来解决。
 */

object G04_VetexMsgAgg {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("GraphX Ops")
    val sc = new SparkContext(sparkConf)

    // 创建顶点，包括自然人和法人
    val vertexSeq = Seq(
      (1L, ("马化腾", "自然人", "50")),
      (2L, ("陈一丹", "自然人", "50")),
      (3L, ("许晨晔", "自然人", "52")),
      (4L, ("张志东", "自然人", "49")),
      (5L, ("深圳市腾讯计算机系统有限公司", "法人", "0")),
      (6L, ("武汉鲨鱼网络直播技术有限公司", "法人", "0")),
      (7L, ("武汉斗鱼网络科技有限公司", "法人", "0")),
      (8L, ("张文明", "自然人", "42")),
      (9L, ("陈少杰", "自然人", "39")),
      (10L, ("深圳市鲨鱼文化科技有限公司", "法人", "0"))
    )
    val vertexSeqRDD: RDD[(VertexId, (String, String, String))] = sc.parallelize(vertexSeq)

    /* 创建边关系，边的属性就是投资金额
    * 边的格式： Edge(srcId, dstId, attr)
    *
    * 其中斗鱼这块认缴资金是虚构的
    */
    val shareEdgeSeq = Seq(
      Edge(1L, 5L, 3528.6), // 马化腾 -> 腾讯
      Edge(2L, 5L, 742.9), // 陈一丹 -> 腾讯
      Edge(3L, 5L, 742.9), // 许晨晔 -> 腾讯
      Edge(4L, 5L, 1485.7), // 张志东 -> 腾讯
      Edge(5L, 6L, 50.0), // 腾讯 -> 鲨鱼
      Edge(7L, 6L, 50.0), // 斗鱼 -> 鲨鱼
      Edge(8L, 7L, 87.5), // 张文明 -> 斗鱼
      Edge(9L, 7L, 1122.2), // 陈少杰 -> 斗鱼
      Edge(6L, 10L, 500.0) // 鲨鱼 -> 深圳鲨鱼
    )
    val shareEdgeRDD: RDD[Edge[Double]] = sc.parallelize(shareEdgeSeq)

    // 构建初始图
    val graph: Graph[(String, String, String), Double] = Graph(vertexSeqRDD, shareEdgeRDD)

    /*
     * CASE 1
     * 如果我们只关心跟腾讯和其上游投资人的关系信息，而不关心下游的斗鱼。
     * 那么我们可以构建一个子图 SubGraph
     *
     * 请注意，有意思的是，子图的所有顶点实际上打印出了所有的顶点，但是三元组里面是不体现的
     * 原因是，我们只构建了子图的边，但是顶点没有做限制，所以顶点作为孤立点存在了子图中
     *
     */
    println("\n================ 打印投资三元组关系 ===================\n")
    graph.triplets.map(
      (triplet: EdgeTriplet[(String, String, String), Double]) =>
        invTypeFunc(triplet)
    ).collect.foreach(println)

    /**
     *
     * @param triplet 三元组 EdgeTriplet[(String, String, String), Double]
     * @return 返回字符串，投资人类型+名称+被投资对象+金额
     */
    def invTypeFunc(triplet: EdgeTriplet[(String, String, String), Double]): String = {
      val invType: String = triplet.srcAttr._2
      val invName: String = triplet.srcAttr._1
      val beInvName: String = triplet.dstAttr._1
      val invMoney: Double = triplet.attr

      invType match {
        case "自然人" => s"投资人是自然人: $invName 投资了 $beInvName ,金额为：$invMoney"
        case "法人" => s"投资人是法人: $invName 投资了 $beInvName ,金额为：$invMoney"
        case "政府机关" => s"投资人是政府机关: $invName 投资了 $beInvName ,金额为：$invMoney"
        case _ => "投资人类型不明确 : " + invName
      }
    }

    /*
     * CASE 2
     *
     * 我们现在想计算一下企业股东的平均年龄
     * 注意这里的投资人特指自然人哦，所以我们需要在消息聚合的时候排除掉非自然人股东。
     *
     * 这里我们依然使用强大的消息聚合函数 aggregateMessages
     *
     *
     */

    val sumAgeOfShareHolders: VertexRDD[(Int, Int)] = graph.aggregateMessages[(Int, Int)](
      triplet => {
        /*
        aggregateMessages 分两步走，第一步发送消息：

        首先判断股东类型，如果股东是自然人：
        1. 将股东的年龄属性转换为int，存入age变量
        2. 将age发送到Dst
         */
        if (triplet.srcAttr._2 == "自然人") {
          val age: Int = triplet.srcAttr._3.toInt
          triplet.sendToDst((1, age))
        }
      },
      /*
      aggregateMessages 分两步走，第二步，聚合消息：

      x._1 是计数
      x._2 是年龄

      计数和计数相加，年龄和年龄相加
       */
      (x: (Int, Int), y: (Int, Int)) => (x._1 + y._1, x._2 + y._2)
    )

    println("\n================ 打印总年龄 ===================\n")
    sumAgeOfShareHolders.collect.foreach(println)

    // 当前我们获得了总年龄和总计数
    // 现在我们开始求平均年龄
    val avgAgeOfShareHolders = sumAgeOfShareHolders.mapValues((id: VertexId, sumAge: (Int, Int)) =>
      sumAge match {
        case (count, sum) => sum / count
      }
    )
    // 打印结果
    println("\n================ 打印股东平均年龄 ===================\n")
    avgAgeOfShareHolders.collect.foreach(println)
  }
}
