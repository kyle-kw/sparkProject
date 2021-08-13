package com.local.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_File_Par {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    /**分区    数据
     * 0   => [1@@2@@]
     * 1   => [3]
     * 2   => []
     */

    val rdd: RDD[String] = sc.textFile("data/input/1.txt",2)

    rdd.saveAsTextFile("data/output/result5")

    sc.stop()
  }
}
