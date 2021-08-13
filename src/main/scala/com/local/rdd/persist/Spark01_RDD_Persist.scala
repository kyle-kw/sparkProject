package com.local.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Persist {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val sc = new SparkContext(conf)

    val list = List("hello scala", "hello spark")

    val rdd: RDD[String] = sc.makeRDD(list)
    val rdd1: RDD[String] = rdd.flatMap(_.split(" "))

    val rdd2: RDD[(String, Int)] = rdd1.map((_, 1))

    // 持久化
    // 缓存
//    rdd2.cache()
    // 保存为临时文件
    rdd2.persist(StorageLevel.DISK_ONLY)

    val reduceRDD: RDD[(String, Int)] = rdd2.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)

    val groupRDD: RDD[(String, Iterable[Int])] = rdd2.groupByKey()
    groupRDD.collect().foreach(println)


    sc.stop()
  }

}
