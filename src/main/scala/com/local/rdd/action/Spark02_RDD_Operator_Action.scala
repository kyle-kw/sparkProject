package com.local.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("action")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // TODO - 行动算子
    // reduce  两两聚合
    println(rdd.reduce(_ + _))

    // collect  会将不同分区的数据，安装分区顺序，采集到Driver端内存中，形成数组
    val  ints: Array[Int] = rdd.collect()
    println(ints.mkString(","))

    // count 计算rdd中数据的个数
    println(rdd.count())

    // first  取数据源当中的第一个数据
    println(rdd.first())

    // take 获取n个数据
    println(rdd.take(2).mkString(","))

    // takeOrdered 数据排序后，获取n个数据
    println(rdd.takeOrdered(2).mkString(","))

    sc.stop()
  }

}
