package com.local.text.driver1

import java.io.{ObjectOutputStream, OutputStream}
import java.net.Socket

object Driver {

  def main(args: Array[String]): Unit = {

    val client = new Socket("localhost", 9999)

    val out: OutputStream = client.getOutputStream
    val objOut = new ObjectOutputStream(out)

    val task = new Task()

    objOut.writeObject(task)
    objOut.flush()
    objOut.close()

    client.close()
    println("客户端数据发送完毕。")
  }
}
