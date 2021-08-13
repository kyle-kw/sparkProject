package com.local.text

class SubTask extends Serializable {

  var datas:List[Int] = _
  var logic:(Int)=>Int = _

  def computer():List[Int] = {
    datas.map(logic)
  }
}
