package com.local.rdd.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_dep {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("data/input/words.txt")
    println(lines.dependencies)
    println("*******************")

    val words: RDD[String] = lines.flatMap(_.split(" "))
    println(words.dependencies)
    println("*******************")

    val wordAndOnes: RDD[(String, Int)] = words.map((_, 1))
    println(wordAndOnes.dependencies)
    println("*******************")

    val result: RDD[(String, Int)] = wordAndOnes.reduceByKey(_ + _)
    println(result.dependencies)
    println("*******************")

    result.collect().foreach(println)
  }
}
