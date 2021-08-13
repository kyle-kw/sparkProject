package com.local.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("action")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))


    // TODO - 行动算子
    /*
    行动算子，其实就是触发作业（job）执行的方法
    底层代码调用的是环境对象的runjob方法
    底层代码会创建ActiveJob，并提交执行
     */
    rdd.collect()

    sc.stop()
  }

}
