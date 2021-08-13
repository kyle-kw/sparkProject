package com.local.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDDemo01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.parallelize(1 to 10)
    val rdd2: RDD[Int] = sc.parallelize(1 to 10, 3)

    val rdd3: RDD[Int] = sc.makeRDD(1 to 10)
    val rdd4: RDD[Int] = sc.makeRDD(1 to 10, 3)

    val rdd5: RDD[String] = sc.textFile("data/input/words.txt")
    val rdd6: RDD[String] = sc.textFile("data/input/words.txt", 3)

    println(rdd1.getNumPartitions)
    println(rdd2.partitions.length)

    println("==============")
    rdd3.foreach(println)
    println("=============")
    println(rdd3.getNumPartitions)
    rdd4.foreach(println)

    println("============")
    println(rdd5.getNumPartitions)
    println("=================")
    rdd6.foreach(println)
    val res: RDD[(String, Int)] = rdd6.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    res.foreach(println)

    res.saveAsTextFile("data/output/result4")

    sc.stop()
  }

}
