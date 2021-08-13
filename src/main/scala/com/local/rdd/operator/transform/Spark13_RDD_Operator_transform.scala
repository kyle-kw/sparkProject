package com.local.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark13_RDD_Operator_transform {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("transform").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // todo 算子--key - value类型
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val rdd: RDD[(Int, Int)] = rdd1.map((_, 1))
    rdd.partitionBy(new HashPartitioner(2)).saveAsTextFile("data/output/result5")


    sc.stop()

  }

}
