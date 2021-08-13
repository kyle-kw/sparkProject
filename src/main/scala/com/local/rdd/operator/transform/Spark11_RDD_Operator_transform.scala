package com.local.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark11_RDD_Operator_transform {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("transform").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 1,2,4,6),1)

    // todo 算子--sordby
    val rdd1: RDD[Int] = rdd.sortBy(num => num)

    rdd1.saveAsTextFile("data/output/result5")
    sc.stop()

  }

}
