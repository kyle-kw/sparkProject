package com.local.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 创建RDD
    //从文件中创建RDD
    val rdd: RDD[String] = sc.textFile("data/input/words.txt")
    val rdd2: RDD[String] = sc.textFile("data/input/words*.txt")
    val rdd3: RDD[(String, String)] = sc.wholeTextFiles("data/input/words*.txt")


    rdd.collect().foreach(println)
    rdd2.collect().foreach(println)
    println("==============")
    rdd3.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }
}
