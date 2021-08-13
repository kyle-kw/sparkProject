package com.local.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark14_RDD_Operator_transform {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("transform").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("c", 4)
    ))
    // todo 算子--key - value类型

    val reduceRDD: RDD[(String, Int)] = rdd.reduceByKey((x:Int, y:Int)=>{
      println(s"x=${x}, y=${y}")
      x+y
    })

    reduceRDD.collect().foreach(println)

    sc.stop()

  }

}
