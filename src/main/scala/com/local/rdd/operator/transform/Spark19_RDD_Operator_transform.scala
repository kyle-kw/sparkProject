package com.local.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark19_RDD_Operator_transform {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("transform").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("c", 2), ("c", 3),
    ))

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 4), ("a", 5), ("c",6)
    ))
    // todo 算子--key - value类型
    //    val sordRDD: RDD[(String, Int)] = rdd.sortByKey()
    val joinRDD: RDD[(String, (Int, Int))] = rdd.join(rdd1)
    joinRDD.collect().foreach(println)

    val leftRDD: RDD[(String, (Int, Option[Int]))] = rdd.leftOuterJoin(rdd1)
    leftRDD.collect().foreach(println)

    val rightRDD: RDD[(String, (Option[Int], Int))] = rdd.rightOuterJoin(rdd1)
    rightRDD.collect().foreach(println)

    val cogRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd.cogroup(rdd1)
    cogRDD.collect().foreach(println)


    sc.stop()

  }

}
