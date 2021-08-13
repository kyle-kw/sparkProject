package com.local.rdd.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_rdd_save {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1),
      ("c", 1),
      ("b", 1)
    ))

    rdd.saveAsTextFile("data/output/result1")
    rdd.saveAsObjectFile("data/output/result2")
    rdd.saveAsSequenceFile("data/output/result3")

    sc.stop()
  }

}
