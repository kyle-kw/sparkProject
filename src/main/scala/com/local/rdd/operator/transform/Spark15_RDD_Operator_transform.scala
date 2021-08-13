package com.local.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark15_RDD_Operator_transform {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("transform").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("c", 4)
    ))
    // todo 算子--key - value类型
    val rdd1: RDD[(String, Iterable[Int])] = rdd.groupByKey()

    rdd1.collect().foreach(println)

    val rdd2: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)

    // spark进行shuffle(洗牌)时，需要将数据写到磁盘中。
    // todo groupByKey,reduceByKey区别
    // 从shuffle的角度来说：
    // reduceByKey会在分组之前进行预聚合（combine），这样shuffle时，和磁盘进行IO操作时，可以减少传输量，增加shuffle效率。
    // 而groupByKey会直接分组，没有进行预聚合，进行shuffle时，和磁盘进行IO操作时，shuffle效率比较慢。
    // 从功能的角度来说：
    // reduceByKey包含分组和聚合的操作，groupByKey只有分组的操作，不能聚合。当只需要分组的情况下，使用groupByKey。
    // 而当需要分组和聚合的操作时，使用reduceByKey。

    sc.stop()

  }

}
