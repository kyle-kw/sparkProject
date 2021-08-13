package com.local.framework.common

import com.local.framework.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}


trait TApplication {

  def start(appName:String="wordCount",master:String="local[*]")(op: =>Unit): Unit ={

    val conf: SparkConf = new SparkConf().setAppName(appName).setMaster(master)
    val sc = new SparkContext(conf)
    EnvUtil.put(sc)

    try{
     op
    }catch {
      case es: Throwable => println(es.getMessage)
    }

    sc.stop()
    EnvUtil.clear()
  }
}
