package com.huag.streaming
import org.apache.log4j.{Level, Logger}

import org.apache.spark.internal.Logging
/**
  * @author huag
  * @date 2019/12/12 10:54
  */
object StreamingExamples extends Logging{

  def setStreamingLogLevels(): Unit ={
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if(!log4jInitialized){
      logInfo("Seting log level to [WARN] for streaming... ")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }

}
