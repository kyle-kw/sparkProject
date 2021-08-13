package com.local.rdd.part

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark01_RDD_Part {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val sc = new SparkContext(conf)

    val rdd: RDD[(String, String)] = sc.makeRDD(List(
      ("nba", "aaaaaaaa"),
      ("cba", "aaaaaaaa"),
      ("wnba", "aaaaaaaa"),
      ("nba", "aaaaaaaa"),
      ("cba", "aaaaaaaa")
    ))
    val partRDD: RDD[(String, String)] = rdd.partitionBy(new MyPartitione)

    partRDD.saveAsTextFile("data/output/result5")


    sc.stop()
  }

  /**
   * 重写Partitioner
   * 1、继承Partitioner
   * 2、重写方法
   */
  class MyPartitione extends Partitioner{

    override def numPartitions: Int = 3

    override def getPartition(key: Any): Int = {
      key match {
        case "nba" => 0
        case "wnba" => 1
        case _ => 2
      }
    }
  }

}
