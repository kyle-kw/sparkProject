package com.local.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("action")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4,4),4)

    // TODO - 行动算子  aggregate
    // aggregateByKey : 初始值只会参与分区内的计算
    // aggregate:初始值不只参与分区内的计算，还会参与分区间的计算
    println(rdd.aggregate(10)(_ + _, _ + _))

    // fold :aggregate的简化版，分区内和分区间相同的计算规则时，可以使用fold
    println(rdd.fold(10)(_ + _))

    // countByValue:统计值的个数
    val intToLong: collection.Map[Int, Long] = rdd.countByValue()
    println(intToLong)

    //countByValue:统计键的个数
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("x", 2)
    ))
    val stringToLong: collection.Map[String, Long] = rdd1.countByKey()
    println(stringToLong)

    val tupleToLong: collection.Map[(String, Int), Long] = rdd1.countByValue()
    println(tupleToLong)


    sc.stop()
  }

}
