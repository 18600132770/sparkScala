package com.huag.flink.streaming.api

import org.apache.flink.api.common.functions.Partitioner

/**
  * @author huag
  * @date 2020/1/17 9:56
  */
class MyPartitionerScala extends Partitioner[Long]{
  override def partition(key: Long, numPartitions: Int): Int = {
    println("分区总数：" + numPartitions)
    if (key % 2 == 0){
      0
    }else{
      1
    }
  }
}
