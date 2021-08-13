package com.local.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_RDD_Operator_transform {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("transform").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 1,2,4,6))

    // map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)
    rdd.distinct().collect().foreach(println)

    println(List(1, 1, 2, 4).distinct)

    sc.stop()

  }

}
