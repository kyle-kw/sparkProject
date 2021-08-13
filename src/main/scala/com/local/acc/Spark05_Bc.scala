package com.local.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

//import scala.collection.parallel.mutable

import scala.collection.mutable

object Spark05_Bc {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 3)
    ))

    val rdd2: mutable.Map[String, Int] = mutable.Map[String, Int](("a", 4), ("b", 5), ("c", 6))

    val rdd1: RDD[(String, (Int, Int))] = rdd.map {
      case (w, c) => {
        val i: Int = rdd2.getOrElse(w, 0)
        (w, (c, i))
      }
    }

    rdd1.collect().foreach(println)

    sc.stop()
  }
}
