package com.local.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.sparkproject.jetty.util.StringUtil
import org.sparkproject.jetty.util.StringUtil.isNotBlank

object RDDDemo02 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("rdd").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("data/input/words.txt")

    val res: RDD[(String, Int)] = lines.filter(isNotBlank).flatMap(_.split(" ")).mapPartitions(iter => {
      iter.map((_, 1))
    }).reduceByKey(_ + _)

    res.foreachPartition(iter => {
      iter.foreach(println)
    })

    res.saveAsTextFile("data/output/result4")

    sc.stop()
  }

}
