package com.local.text

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

object Executor {
  def main(args: Array[String]): Unit = {
    val server1 = new ServerSocket(9999)
    val server2 = new ServerSocket(8888)
    println("客户端启动，等待连接...")

    val client1: Socket = server1.accept()
    val in1: InputStream = client1.getInputStream

    val objIn1 = new ObjectInputStream(in1)
    val subTask1: SubTask = objIn1.readObject().asInstanceOf[SubTask]

    val ints1: List[Int] = subTask1.computer()

    println("计算节点1,计算的结果为：" + ints1)
    objIn1.close()
    client1.close()
    server1.close()

    val client2: Socket = server2.accept()
    val in2: InputStream = client2.getInputStream

    val objIn2 = new ObjectInputStream(in2)
    val subTask2: SubTask = objIn2.readObject().asInstanceOf[SubTask]

    val ints2: List[Int] = subTask2.computer()

    println("计算节点2,计算的结果为：" + ints2)
    objIn1.close()
    client1.close()
    server1.close()
  }
}
