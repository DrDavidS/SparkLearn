object MapCompare {
  def main(args: Array[String]): Unit = {
    val immutableMap = Map("Jim" -> 22, "yxj" -> 32)
    val stringToInt: Map[String, Int] = immutableMap.map((row: (String, Int)) => (row._1, row._2))
    println(stringToInt)
    println(immutableMap)
  }
}
