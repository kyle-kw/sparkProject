package com.local.text.driver1

import java.io.{InputStream, ObjectInputStream, ObjectOutputStream}
import java.net.{ServerSocket, Socket}

object Executor {

  def main(args: Array[String]): Unit = {
    val server = new ServerSocket(9999)
    println("服务器启动，等待接收数据")

    val client: Socket = server.accept()

    val in: InputStream = client.getInputStream
    val objIn = new ObjectInputStream(in)

    val task:Task = objIn.readObject().asInstanceOf[Task]
    val ints: List[Int] = task.computer()

    println("计算节点计算的结果：" + ints.mkString(","))

    objIn.close()
    client.close()
    server.close()

  }
}
