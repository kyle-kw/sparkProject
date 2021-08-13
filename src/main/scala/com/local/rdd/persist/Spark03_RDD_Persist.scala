package com.local.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Persist {

  def main(args: Array[String]): Unit = {

    /**
     * cache: 将数据储存在内存中进线数据重用
     *        会在血缘关系中，添加新的依赖，一旦出现问题，可以从头读取数据
     *
     * persist: 将数据临时储存在磁盘文件中进行数据重用
     *         涉及到磁盘IO，性能比较低，但是数据安全
     *         如果作业执行完毕，临时保存的数据文件就会丢失
     *
     *
     * chechpoint: 将数据长久地保存在磁盘文件当中进行数据重用
     *            涉及到磁盘IO，性能比较低，但是数据安全
     *            为了保证数据安全，一般情况下，会独立执行作业
     *            为了能够提高效率，一般需要和cache联合使用，这样就不会产生独立作业
     *            执行过程中，会切断血缘关系，重新建立血缘关系
     *            会改变数据源
     */

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("data/output/result5")

    val list = List("hello scala", "hello spark")

    val rdd: RDD[String] = sc.makeRDD(list)
    val rdd1: RDD[String] = rdd.flatMap(_.split(" "))

    val rdd2: RDD[(String, Int)] = rdd1.map((_, 1))

    // 设置检查点 checkpoint 需要落盘，需要指定检查点保存路径setCheckpointDir
    // 检查点保存的文件，当作业执行完毕后，不会被删除
    // 一般保存路径都是在分布式储存系统中：HDFS
    rdd2.cache()
    rdd2.checkpoint()

    println(rdd2.toDebugString)
    val reduceRDD: RDD[(String, Int)] = rdd2.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)
    println(rdd2.toDebugString)

//    val groupRDD: RDD[(String, Iterable[Int])] = rdd2.groupByKey()
//    groupRDD.collect().foreach(println)


    sc.stop()
  }

}
