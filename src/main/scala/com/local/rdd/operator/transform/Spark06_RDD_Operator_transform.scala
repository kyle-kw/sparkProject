package com.local.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_transform {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("transform").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List("hadoop","hello","spark","scala"))

    //todo groupBy分组
    //    、、val groupRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(_ % 2)

    val groupRDD: RDD[(Char, Iterable[String])] = rdd.groupBy(_.charAt(0))

    groupRDD.collect().foreach(println)

    sc.stop()

  }

}
