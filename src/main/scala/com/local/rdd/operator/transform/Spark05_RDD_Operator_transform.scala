package com.local.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_transform {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("transform").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

    val glomRDD: RDD[Array[Int]] = rdd.glom()

//    println(glomRDD.collect().mkString(","))
    glomRDD.collect().foreach(data => println(data.mkString(",")))


    sc.stop()

  }

}
