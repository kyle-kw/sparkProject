package com.local.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Memory_Par {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 创建RDD
    //从内存中创建RDD,并制定分区
    val rdd1: RDD[Int] = sc.makeRDD(
      List(1, 2, 3, 4,5,6,7),3
    )

    rdd1.saveAsTextFile("data/output/result5")

    //TODO 关闭环境
    sc.stop()
  }
}
