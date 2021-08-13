package com.local.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_transform1 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("transform").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List(
      "Hello World", "Hello Spark"
    ))

    //todo 扁平化
    val rdd1: RDD[String] = rdd.flatMap(l => l.split(" "))

    val rdd2: RDD[(String, Int)] = rdd1.map((_, 1)).reduceByKey(_ + _)


    rdd1.collect().foreach(println)
    rdd2.collect().foreach(println)


    sc.stop()

  }

}
