package com.local.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Operator_transform_text {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("transform").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("data/input/data/apache.log")
    //todo filter
    rdd.filter(
      line => {
        val time: String = line.split(" ")(3)
        time.startsWith("17/05/2015")
      }
    ).collect().foreach(println)




    sc.stop()

  }

}
