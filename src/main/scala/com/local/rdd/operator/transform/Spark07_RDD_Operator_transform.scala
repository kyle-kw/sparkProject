package com.local.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Operator_transform {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("transform").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List("hadoop","hello","spark","scala"))

    //todo filter
    val filterRDD: RDD[String] = rdd.filter(_.charAt(0).equals('h'))

    filterRDD.collect().foreach(println)

    sc.stop()

  }

}
