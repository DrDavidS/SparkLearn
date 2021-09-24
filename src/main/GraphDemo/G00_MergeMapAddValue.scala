// https://stackoverflow.com/questions/7076128/best-way-to-merge-two-maps-and-sum-the-values-of-same-key
object G00_MergeMapAddValue {
  def main(args: Array[String]): Unit = {
    val leftMap = Map(1 -> "10", 2 -> "20")
    val rightMap = Map(1 -> "10", 2 -> "20")

    val intToString: Map[Int, String] = leftMap ++ rightMap.map {
      case (k, v) =>
        k -> (v.toInt + leftMap.getOrElse(k, "0").toInt).toString
    }

    println("intToString  " + intToString)
    println(leftMap == rightMap)
    println(leftMap != rightMap)
  }
}
