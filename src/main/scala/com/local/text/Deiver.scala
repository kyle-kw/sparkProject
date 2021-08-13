package com.local.text

import java.io.{ObjectOutputStream, OutputStream}
import java.net.Socket

object Deiver {

  def main(args: Array[String]): Unit = {
    val client1 = new Socket("localhost",9999)
    val client2 = new Socket("localhost",8888)

    val task = new Task()
    val out1: OutputStream = client1.getOutputStream
    val out2: OutputStream = client2.getOutputStream
    val objOut1 = new ObjectOutputStream(out1)
    val objOut2 = new ObjectOutputStream(out2)

    val subTask = new SubTask()
    subTask.logic = task.logic
    subTask.datas = task.datas.take(2)

    objOut1.writeObject(subTask)
    objOut1.flush()
    objOut1.close()
    client1.close()

    subTask.datas = task.datas.takeRight(2)
    objOut2.writeObject(subTask)
    objOut2.flush()
    objOut2.close()
    objOut2.close()

    println("客户端数据发送完毕。")
  }

}
