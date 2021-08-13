package com.local.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Persist {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("data/output/result5")

    val list = List("hello scala", "hello spark")

    val rdd: RDD[String] = sc.makeRDD(list)
    val rdd1: RDD[String] = rdd.flatMap(_.split(" "))

    val rdd2: RDD[(String, Int)] = rdd1.map((_, 1))

    // 设置检查点 checkpoint 需要落盘，需要指定检查点保存路径setCheckpointDir
    // 检查点保存的文件，当作业执行完毕后，不会被删除
    // 一般保存路径都是在分布式储存系统中：HDFS
    rdd2.checkpoint()

    val reduceRDD: RDD[(String, Int)] = rdd2.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)

    val groupRDD: RDD[(String, Iterable[Int])] = rdd2.groupByKey()
    groupRDD.collect().foreach(println)


    sc.stop()
  }

}
