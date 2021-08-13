package com.local.text

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("data/input/words.txt")
    val words: RDD[String] = lines.flatMap(_.split(" "))
    val wordsToOne: RDD[(String, Int)] = words.map((_, 1))
    val res: RDD[(String, Int)] = wordsToOne.reduceByKey(_ + _)

    res.collect().foreach(println)

    sc.stop()
  }
}
