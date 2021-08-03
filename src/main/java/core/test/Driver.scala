package core.test

import java.io.{ObjectOutputStream, OutputStream}
import java.net.Socket

object Driver {
  def main(args: Array[String]): Unit = {
    // 连接服务器
    // 两个计算节点
    val client1 = new Socket("localhost", 9999)
    val client2 = new Socket("localhost", 8888)

    val task = new Task()

    // 发送给Executor1
    val out1: OutputStream = client1.getOutputStream
    val objOut1 = new ObjectOutputStream(out1)
    // 拆分，subTask拿前半部
    val subTask = new SubTask()
    subTask.logic = task.logic
    subTask.datum = task.datum.take(2)

    objOut1.writeObject(subTask)
    objOut1.flush()
    objOut1.close()

    client1.close()
    println("客户端1数据发送完毕")


    // 发送给Executor2
    val out2: OutputStream = client2.getOutputStream
    val objOut2 = new ObjectOutputStream(out2)
    // 拆分，subTask2拿后半部
    val subTask2 = new SubTask()
    subTask2.logic = task.logic
    subTask2.datum = task.datum.takeRight(2)

    objOut2.writeObject(subTask2)
    objOut2.flush()
    objOut2.close()

    client2.close()
    println("客户端2数据发送完毕")
  }
}
