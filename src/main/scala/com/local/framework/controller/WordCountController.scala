package com.local.framework.controller


import com.local.framework.common.TController
import com.local.framework.service.WordCountService

// TODO 控制层，调度层
class WordCountController extends TController{

  private val wordCountService = new WordCountService()

  // 调度代码
  def dispath(): Unit ={
    val res: Array[(String, Int)] = wordCountService.dataAnalysis()
    res.foreach(println)
  }

}
