package com.local.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark16_RDD_Operator_transform {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("transform").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("a", 4)
    ),2)
    // todo 算子--key - value类型
    // aggregateByKey 算子是函数柯里化，存在两个参数列表
    // aggregateByKey分区内和分区间做不同的操作。
    // 第一个参数列表：初始值

    // 第二个参数列表：
    //    第一个参数：分区内的计算规则
    //    第二个参数：分区间的计算规则
    val rdd1: RDD[(String, Int)] = rdd.aggregateByKey(5)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    )

    rdd1.collect().foreach(println)

    sc.stop()

  }

}
