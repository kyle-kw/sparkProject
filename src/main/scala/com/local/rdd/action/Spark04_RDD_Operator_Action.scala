package com.local.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("action")
    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 3)
    ))

    // TODO - 行动算子
    rdd.saveAsTextFile("data/output/output1")
    rdd.saveAsObjectFile("data/output/output2")
    rdd.saveAsSequenceFile("data/output/output3")

    sc.stop()
  }

}
