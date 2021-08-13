package com.local.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 创建RDD
    //从内存中创建RDD
    val rdd1: RDD[Int] = sc.parallelize(List(1, 2, 3, 4))
    val seq: Seq[Int] = Seq[Int](1,2,3,4)
    val rdd2: RDD[Int] = sc.parallelize(seq)
    val rdd3: RDD[Int] = sc.makeRDD(seq)

    rdd2.collect().foreach(println)
    println("========")
    rdd3.collect().foreach(println)


    //TODO 关闭环境
    sc.stop()
  }
}
