package core.test

class SubTask extends Serializable {
  var datum: List[Int] = _
  var logic: Int => Int = _  // 这里接受的是Task中logic算好的值

  // 计算
  def compute(): List[Int] = {
    datum.map(logic)
  }
}
