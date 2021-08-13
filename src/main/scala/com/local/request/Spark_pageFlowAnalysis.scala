package com.local.request

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_pageFlowAnalysis {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("top10").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("data/input/user_visit_action.txt")
    val mapRDD: RDD[UserVisitAction] = rdd.map(
      lines => {
        val datas: Array[String] = lines.split("_")
        UserVisitAction(
          datas(0),
          datas(1).toLong,
          datas(2),
          datas(3).toLong,
          datas(4),
          datas(5),
          datas(6).toLong,
          datas(7).toLong,
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12).toLong
        )
      }
    )
    mapRDD.cache()

    // todo 分母
    val pageMapRDD: Map[Long, Long] = mapRDD.map(
      iter => {
        (iter.page_id, 1L)
      }
    ).reduceByKey(_ + _).collect().toMap


    // todo 分子
    val pageToPageRDD: RDD[(String, List[((Long, Long), Int)])] = mapRDD.groupBy(_.session_id).mapValues(
      iter => {
        val actions: List[UserVisitAction] = iter.toList.sortBy(_.action_time)
        val longs: List[Long] = actions.map(_.page_id)
        val tuples: List[(Long, Long)] = longs.zip(longs.tail)
        tuples.map((_, 1))
      }
    )

    val pageToPage_1RDD: RDD[((Long, Long), Int)] = pageToPageRDD.map(_._2).flatMap(iter => iter).reduceByKey(_ + _)

    pageToPage_1RDD.foreach{
      case ((id1,id2),sum)=>{
        val l: Long = pageMapRDD.getOrElse(id1, 0L)
        println(s"由页面${id1}跳转到${id2}的跳转率为: ${(sum.toDouble/l*100).formatted("%.2f")}%")
      }
    }

    sc.stop()
  }
  case class UserVisitAction(
    date: String,//用户点击行为的日期
    user_id: Long,//用户的 ID
    session_id: String,//Session 的 ID
    page_id: Long,//某个页面的 ID
    action_time: String,//动作的时间点
    search_keyword: String,//用户搜索的关键词
    click_category_id: Long,//某一个商品品类的 ID
    click_product_id: Long,//某一个商品的 ID
    order_category_ids: String,//一次订单中所有品类的 ID 集合
    order_product_ids: String,//一次订单中所有商品的 ID 集合
    pay_category_ids: String,//一次支付中所有品类的 ID 集合
    pay_product_ids: String,//一次支付中所有商品的 ID 集合
    city_id: Long
  )

}
