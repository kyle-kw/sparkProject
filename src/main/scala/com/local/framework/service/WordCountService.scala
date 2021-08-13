package com.local.framework.service

import com.local.framework.common.TService
import com.local.framework.dao.WordCountDao
import org.apache.spark.rdd.RDD

// TODO 逻辑层
class WordCountService extends TService{

  private val wordCountDao = new WordCountDao

  // 运行的逻辑代码
  def dataAnalysis() ={
    val lines: RDD[String] = wordCountDao.readFile("data/input/words.txt")
    val words = lines.flatMap(_.split(" "))
    val wa = words.map((_, 1))
    val res = wa.reduceByKey(_ + _).collect()
    res
  }
}
