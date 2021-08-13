package com.local.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("action")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)

    // TODO - 行动算子
    val user = new User()

    rdd.foreach(
      num => {
        println("age = "+(user.age+num))
      }
    )

    sc.stop()
  }
//  class User() extends Serializable {
//    val age:Int = 30;
//  }

  case class User(){
    val age:Int = 30;
  }

}
