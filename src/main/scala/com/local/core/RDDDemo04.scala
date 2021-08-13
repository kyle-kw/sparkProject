package com.local.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDDemo04 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.parallelize(1 to 10)
    val rdd2: RDD[Int] = sc.parallelize(1 to 10, 3)

    println(rdd1.sum())
    println(rdd1.reduce(_+_))

    println(rdd2.sum())
    println()
    println(rdd1.getNumPartitions)
    println(rdd2.getNumPartitions)

    sc.stop()
  }

}
