object MapCompare {
  def main(args: Array[String]): Unit = {
    val leftMap = Map(0 -> "test0", 1 -> "test1")
    val rightMap = Map(0 -> "test0", 1 -> "test1")
    val rightMap2 = Map(0 -> "test0", 1 -> "test111")
    if (leftMap == rightMap2) println("true") else println("false")
  }

}
