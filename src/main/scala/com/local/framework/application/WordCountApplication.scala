package com.local.framework.application

import com.local.framework.common.TApplication
import com.local.framework.controller.WordCountController

object WordCountApplication extends App with TApplication{

  start(){
    val wordCountController = new WordCountController
    wordCountController.dispath()
  }

}
