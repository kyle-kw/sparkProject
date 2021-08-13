package com.local.request

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_top10 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("top10").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("data/input/user_visit_action.txt")
    rdd.cache()

    // 分割数据，需要商品id，点击、下单、支付（id,(点击,下单,支付)）

    // 点击
    val clickNum: RDD[String] = rdd.filter(
      action => {
        val datas: Array[String] = action.split("_")
        datas(6) != "-1"
      }
    )
    val actionClick: RDD[(String, (Int,Int,Int))] = clickNum.map(
      action => {
        (action.split("_")(6), (1,0,0))
      }
    )


    // 下单
    val orderNum: RDD[String] = rdd.filter(
      action => {
        val datas: Array[String] = action.split("_")
        datas(8) != "null"
      }
    )
    val actionOrder: RDD[(String, (Int,Int,Int))] = orderNum.flatMap(
      action => {
        val datas: Array[String] = action.split("_")
        val cid: String = datas(8)
        val cids: Array[String] = cid.split(",")
        cids.map((_, (0,1,0)))
      }
    )


    // 支付
    val payNum: RDD[String] = rdd.filter(
      action => {
        val datas: Array[String] = action.split("_")
        datas(10) != "null"
      }
    )
    val payOrder: RDD[(String, (Int,Int,Int))] = payNum.flatMap(
      action => {
        val datas: Array[String] = action.split("_")
        val cid: String = datas(10)
        val cids: Array[String] = cid.split(",")
        cids.map((_, (0,0,1)))
      }
    )

    val resRDD: RDD[(String, (Int, Int, Int))] = actionClick.union(actionOrder).union(payOrder)

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
