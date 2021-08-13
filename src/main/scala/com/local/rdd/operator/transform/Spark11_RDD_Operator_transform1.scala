package com.local.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark11_RDD_Operator_transform1 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("transform").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("1", 1), ("11", 2), ("2", 3)))

    // todo 算子--sordby
    val rdd1: RDD[(String, Int)] = rdd.sortBy(num => num._1.toInt,false)
    rdd1.collect().foreach(println)
    sc.stop()

  }

}
