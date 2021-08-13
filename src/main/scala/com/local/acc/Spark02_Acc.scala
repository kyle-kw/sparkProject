package com.local.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Acc {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // 创建累加器
    val sumAcc: LongAccumulator = sc.longAccumulator("sum")

    rdd.foreach(
      num =>{
        // 使用累加器
        sumAcc.add(num)
      }
    )

    println(sumAcc.value)

    sc.stop()
  }

}
