package com.local.rdd.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_rdd_load {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val sc = new SparkContext(conf)

    val rdd1: RDD[String] = sc.textFile("data/output/result1")
    val rdd2: RDD[(String,Int)] = sc.objectFile("data/output/result2")
    val rdd3: RDD[(String, Int)] = sc.sequenceFile[String, Int]("data/output/result3")

    println(rdd1.collect().mkString(","))
    println(rdd2.collect().mkString(","))
    println(rdd3.collect().mkString(","))

    sc.stop()
  }

}
