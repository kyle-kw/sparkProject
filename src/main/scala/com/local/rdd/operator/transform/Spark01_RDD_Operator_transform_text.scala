package com.local.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_transform_text {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("transform").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("data/input/data/apache.log")

    val rdd1: RDD[String] = rdd.map(_.split(" ")(6))

    rdd1.collect().foreach(println)


    sc.stop()

  }

}
