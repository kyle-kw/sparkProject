package com.local.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

//import scala.collection.parallel.mutable

import scala.collection.mutable

object Spark04_Acc_WordCount {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List("hello","spark","hello"))

    // 创建累加器
    val wcAcc = new MyAccumulator()
    sc.register(wcAcc,"WordConutAcc")

    rdd.foreach(
      word => {
        wcAcc.add(word)
      }
    )

    println(wcAcc.value)

    sc.stop()
  }

  class MyAccumulator extends AccumulatorV2[String, mutable.Map[String,Long]]{

    private var mapAcc  = mutable.Map[String,Long]()

    // 判断初始状态
    override def isZero: Boolean = {
      mapAcc.isEmpty
    }

    // 复制出来一个
    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAccumulator()
    }

    // 清空
    override def reset(): Unit = {
      mapAcc.clear()
    }

    // 获取累加器的值
    override def add(v: String): Unit = {
      val newnum: Long = mapAcc.getOrElse(v, 0L) + 1
      mapAcc.update(v,newnum)
    }

    // 合并累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {

      other.value.foreach{
        case (word,count)=>{
          val newCount: Long = mapAcc.getOrElse(word,0L)+count
          mapAcc.update(word,newCount)
        }
      }
    }

    // 累加器结果
    override def value: mutable.Map[String, Long] = {
      mapAcc
    }
  }
}
