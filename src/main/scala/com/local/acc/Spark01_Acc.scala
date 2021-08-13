package com.local.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Acc {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    val i: Int = rdd.reduce(_ + _)
    println(i)

    var sum = 0
    rdd.foreach(
      num=>{
        sum += num
      }
    )
    println("sum = "+sum)

    sc.stop()
  }

}
