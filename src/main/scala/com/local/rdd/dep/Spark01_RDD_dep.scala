package com.local.rdd.dep

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark01_RDD_dep {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("data/input/words.txt")
    println(lines.toDebugString)
    println("*******************")

    val words: RDD[String] = lines.flatMap(_.split(" "))
    println(words.toDebugString)
    println("*******************")

    val wordAndOnes: RDD[(String, Int)] = words.map((_, 1))
    println(wordAndOnes.toDebugString)
    println("*******************")

    val result: RDD[(String, Int)] = wordAndOnes.reduceByKey(_ + _)
    println(result.toDebugString)
    println("*******************")

    result.collect().foreach(println)
  }
}
