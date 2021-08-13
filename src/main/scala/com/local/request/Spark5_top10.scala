package com.local.request

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark5_top10 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("top10").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val hotAcc = new HotCateAccumulator
    sc.register(hotAcc)

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



    resRDD.foreach{
      case (st,(x1,x2,x3))=>{
        hotAcc.add((st,x1,x2,x3))
      }
    }
    val sumCount: List[(String, (Int, Int, Int))] = hotAcc.value.toList
    val top10: List[(String, (Int, Int, Int))] = sumCount.sortBy(_._2)(Ordering.Tuple3(Ordering.Int.reverse,Ordering.Int.reverse,Ordering.Int.reverse)).take(10)
    // 排序


    top10.foreach(println)
    sc.stop()
  }


  class HotCateAccumulator extends AccumulatorV2[(String,Int,Int,Int),mutable.Map[String,(Int,Int,Int)]]{

    private var mapAcc  = mutable.Map[String,(Int,Int,Int)]()

    override def isZero: Boolean = {
      mapAcc.isEmpty
    }

    override def copy(): AccumulatorV2[(String, Int, Int, Int), mutable.Map[String, (Int, Int, Int)]] = {
      new HotCateAccumulator
    }

    override def reset(): Unit = {
      mapAcc.clear()
    }

    override def add(v: (String, Int, Int, Int)): Unit = {
      val data: (Int, Int, Int) = mapAcc.getOrElse(v._1, (0, 0, 0))
      mapAcc.update(v._1,(v._2+data._1,v._3+data._2,v._4+data._3))
    }

    override def merge(other: AccumulatorV2[(String, Int, Int, Int), mutable.Map[String, (Int, Int, Int)]]): Unit = {
      other.value.foreach{
        case (st,(x1,x2,x3))=>{
          val data: (Int, Int, Int) = mapAcc.getOrElse(st, (0, 0, 0))
          mapAcc.update(st,(x1+data._1,x2+data._2,x3+data._3))
        }
      }
    }

    override def value: mutable.Map[String, (Int, Int, Int)] = (
      mapAcc
    )
  }

}
