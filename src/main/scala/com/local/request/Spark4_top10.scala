package com.local.request

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark4_top10 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("top10").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("data/input/user_visit_action.txt")

    val resRDD: RDD[(String, (Int, Int, Int))] = rdd.flatMap(
      lines => {
        val datas: Array[String] = lines.split("_")
        if (datas(6) != "-1") {
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != "null") {
          val ids: Array[String] = datas(8).split(",")
          ids.map((_, (0, 1, 0)))
        } else if (datas(10) != "null") {
          val ids: Array[String] = datas(10).split(",")
          ids.map((_, (0, 0, 1)))
        } else {
          Nil
        }
      }
    )

    val sumCount: RDD[(String, (Int, Int, Int))] = resRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )

    // 排序
    val top10: Array[(String, (Int, Int, Int))] = sumCount.sortBy(_._2, false).take(10)

    top10.foreach(println)
    sc.stop()
  }

}
