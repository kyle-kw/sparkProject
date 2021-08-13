package com.local.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_transform_text {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("transform").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("data/input/data/apache.log")

    val rdd1: RDD[(String, Int)] = rdd.map(line => (line.split(" ")(3).split(":")(1), 1))

    //todo groupBy分组
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = rdd1.groupBy(_._1)


    val groupRDD1: RDD[(String, Int)] = groupRDD.map {
      case (st, iter) => {
        (st, iter.size)
      }
    }

    groupRDD1.collect().foreach(println)

    sc.stop()

  }

}
