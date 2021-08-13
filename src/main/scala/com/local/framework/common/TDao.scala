package com.local.framework.common

import com.local.framework.util.EnvUtil

trait TDao {

  def readFile(path:String) ={
    EnvUtil.take().textFile(path)
  }
}
