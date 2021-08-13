package com.local.text.driver1

class Task extends Serializable {

  val datas = List(1,2,3,4)

  val logic:(Int) => Int = _ * 2

  def computer() ={
    datas.map(logic)
  }
}
