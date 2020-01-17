package com.huag.flink.streaming.customSource

import org.apache.flink.streaming.api.functions.source.SourceFunction


/**
  * @author huag
  * @date 2020/1/17 9:18
  */
class MyNoParallelSourceScala extends SourceFunction[Long]{

  var count = 1L
  var isRunning = true

  override def run(sourceContext: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning){
      sourceContext.collect(count)
      count+=1
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
