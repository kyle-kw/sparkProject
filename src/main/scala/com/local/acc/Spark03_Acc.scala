package com.local.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Acc {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // 创建累加器
    val sumAcc: LongAccumulator = sc.longAccumulator("sum")

    val mapRDD: RDD[Int] = rdd.map(
      num => {
        // 使用累加器
        sumAcc.add(num)
        num
      }
    )

    //获取累加器的值
    // 少加： 转换算子中调用累加器，如果没有行动算子的话，那就不会执行
    // 多加： 转换算子中调用累加器，如果多次调用行动算子的话，那就多次调用累加器，就会多加
    // 一般情况下，累加器放在行动算子中操作
    println(sumAcc.value)

    sc.stop()
  }

}
