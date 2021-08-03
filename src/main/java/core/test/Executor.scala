package core.test

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

/**
 * 执行节点1
 * 运行时，先启动SocketServer，再启动SocketClient。
 *
 * 在先启动SocketServer时，代码执行到socket.receive(packet)时
 * 会一直阻塞在这里，直到启动SocketClient后，
 * SocketServer会继续执行，并将收到SocketClient的信息打印出来
 */

// 先执行 Executor，再启动Driver
object Executor {
  def main(args: Array[String]): Unit = {
    // 启动服务器接收数据
    val server = new ServerSocket(9999) // 服务端监听端口9999，现在处于等待连接的状态
    println("服务器[9999]启动，等待接收数据")

    // 等待客户端的连接
    val client: Socket = server.accept() // 使用accept监听来自客户端的连接
    val in: InputStream = client.getInputStream // 获取字节流
    val objIn = new ObjectInputStream(in) // 从获取的字节流中创建一个反序列化流
    val task: SubTask = objIn.readObject().asInstanceOf[SubTask]
    val ints: List[Int] = task.compute()  // 计算

    println("计算节点[9999]的计算结果为： " + ints)

    objIn.close()
    client.close() // 关闭客户端
    server.close() // 关闭server
  }
}
