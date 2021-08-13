package com.local.rdd.operator.transform

import breeze.linalg.max
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_transform_text {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("transform").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)

    val rdd1: RDD[Int] = rdd.mapPartitions(iter => {
//      iter.filter(_ == iter.max)
      List(iter.max).iterator
    })

    rdd1.collect().foreach(println)

    sc.stop()

  }

}
