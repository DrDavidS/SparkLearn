package core.test

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}


// 先执行 Executor，再启动Driver
object Executor {
  def main(args: Array[String]): Unit = {
    // 启动服务器接收数据
    val server = new ServerSocket(9999)
    println("服务器[9999]启动，等待接收数据")

    // 等待客户端的连接
    val client: Socket = server.accept()
    val in: InputStream = client.getInputStream
    val objIn = new ObjectInputStream(in)
    val task: SubTask = objIn.readObject().asInstanceOf[SubTask]
    val ints: List[Int] = task.compute()  // 计算

    println("计算节点[9999]的计算结果为： " + ints)

    objIn.close()
    client.close() // 关闭客户端
    server.close() // 关闭server
  }
}
