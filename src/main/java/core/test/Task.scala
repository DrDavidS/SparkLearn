package core.test


// 注意，需要可以序列化，所以要继承 Serializable
class Task extends Serializable {
  val datum = List(1, 2, 3, 4)
  val logic: Int => Int = (_: Int) * 2

  // 计算
  def compute(): List[Int] = {
    datum.map(logic)
  }
}
