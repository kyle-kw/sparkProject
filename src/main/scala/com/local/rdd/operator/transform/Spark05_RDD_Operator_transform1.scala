package com.local.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_transform1 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("transform").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("data/input/words.txt")

    val rdd1: RDD[Array[String]] = rdd.glom()

    rdd1.collect().foreach(data => println(data.mkString(",")))


    sc.stop()

  }

}
