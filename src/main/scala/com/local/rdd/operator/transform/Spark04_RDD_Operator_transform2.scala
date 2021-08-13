package com.local.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_transform2 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("transform").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[Any] = sc.makeRDD(List(List(1, 2), 3, List(4, 5)))

    //todo 扁平化
    val rdd1: RDD[Any] =rdd.flatMap {
      case list: List[_] => list
      case dat => List(dat)
    }

    rdd1.collect().foreach(println)



    sc.stop()

  }

}
