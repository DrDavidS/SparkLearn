package core.test

import java.io.{ObjectOutputStream, OutputStream}
import java.net.Socket

object Driver {
  def main(args: Array[String]): Unit = {
    // 连接服务器
    // 两个计算节点
    val client1 = new Socket("localhost", 9999) // 客户端1请求
    val client2 = new Socket("localhost", 8888) // 客户端2请求

    val task = new Task()

    // 发送给Executor1
    val out1: OutputStream = client1.getOutputStream
    val objOut1 = new ObjectOutputStream(out1) //将指定的对象写入 ObjectOutputStream
    // 拆分，subTask拿前半部
    val subTask = new SubTask()
    subTask.logic = task.logic
    subTask.datum = task.datum.take(2)

    objOut1.writeObject(subTask)
    objOut1.flush() // 清空缓冲区
    objOut1.close() // 关闭IO流

    client1.close()
    println("客户端1数据发送完毕")


    // 发送给Executor2
    val out2: OutputStream = client2.getOutputStream
    val objOut2 = new ObjectOutputStream(out2) //将指定的对象写入 ObjectOutputStream
    // 拆分，subTask2拿后半部
    val subTask2 = new SubTask()
    subTask2.logic = task.logic
    subTask2.datum = task.datum.takeRight(2)

    objOut2.writeObject(subTask2)
    objOut2.flush() // 清空缓冲区
    objOut2.close() // 关闭IO流

    client2.close()
    println("客户端2数据发送完毕")
  }
}
