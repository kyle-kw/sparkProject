package com.local.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_RDD_Operator_Transform_text1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("text").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("data/input/data/agent.log")

    val rdd1: RDD[((String, String), Int)] = rdd.map(line => ((line.split(" ")(1), line.split(" ")(4)), 1))

    val rdd2: RDD[((String, String), Int)] = rdd1.reduceByKey(_ + _)

    val rdd3: RDD[(String, (String, Int))] = rdd2.map {
      case ((x, y), z) => {
        (x, (y, z))
      }
    }

    val rdd4: RDD[(String, Iterable[(String, Int)])] = rdd3.groupByKey()

    val rdd5: RDD[(String, List[(String, Int)])] = rdd4.mapValues(iter => {
      val tuples: List[(String, Int)] = iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      tuples
    })


    rdd5.collect().foreach(println)

    sc.stop()

  }

}
