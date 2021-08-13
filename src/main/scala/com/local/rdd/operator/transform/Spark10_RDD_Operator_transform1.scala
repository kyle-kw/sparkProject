package com.local.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_RDD_Operator_transform1 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("transform").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6),2)

    // todo 改变分区
    // 扩大分区使用repartition，底层使用的是coalesce(numPartitions, shuffle = true)
    // 缩减分区使用coalesce，如果想要数据均衡，可以使用shuffle
    val rdd1: RDD[Int] = rdd.repartition(3)

    rdd1.saveAsTextFile("data/output/result5")


    sc.stop()

  }

}
